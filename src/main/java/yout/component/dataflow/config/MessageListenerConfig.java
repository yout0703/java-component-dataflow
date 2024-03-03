package yout.component.dataflow.config;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.lang.tree.Tree;
import cn.hutool.core.lang.tree.TreeNode;
import cn.hutool.core.lang.tree.TreeUtil;
import cn.hutool.core.map.MapUtil;
import cn.hutool.core.util.StrUtil;
import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.core.QueueInformation;
import org.springframework.amqp.rabbit.annotation.RabbitListenerConfigurer;
import org.springframework.amqp.rabbit.batch.SimpleBatchingStrategy;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerEndpoint;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.listener.RabbitListenerEndpointRegistrar;
import org.springframework.aop.support.AopUtils;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.StringUtils;
import yout.component.dataflow.annotation.Flow;
import yout.component.dataflow.annotation.Step;
import yout.component.dataflow.context.DataflowContext;
import yout.component.dataflow.processor.DataflowReceiver;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * 消息监听设置，等同于 spring-rabbit 的 @RabbitListener
 *
 * @author yout0703
 */
@Configuration
@Slf4j
public class MessageListenerConfig implements RabbitListenerConfigurer {

    @Resource
    private DataflowContext dataflowContext;

    @Resource
    private SimpleRabbitListenerContainerFactory dataflowDefaultContainerFactory;

    @Resource
    private DataflowReceiver dataflowReceiver;

    @Resource
    private ApplicationContext applicationContext;

    @Resource
    private RabbitAdmin rabbitAdmin;

    @Resource
    private DataflowConfig dataflowConfig;

    @Override
    public void configureRabbitListeners(RabbitListenerEndpointRegistrar registrar) {
        registrar.setContainerFactoryBeanName("dataflowDefaultContainerFactory");
        registrar.setContainerFactory(dataflowDefaultContainerFactory);
        if (!DataflowContext.initialized) {
            initFlowContext();
        }
        if (MapUtil.isNotEmpty(dataflowContext.getDataFlowJobsMap())) {
            dataflowContext.getDataFlowJobsMap()
                    .forEach((flowName, jobNames) -> {
                        if (CollUtil.isEmpty(jobNames) || StrUtil.isBlank(flowName)) {
                            return;
                        }
                        jobNames.forEach(i -> {
                            SimpleRabbitListenerEndpoint endpoint = new SimpleRabbitListenerEndpoint();
                            endpoint.setId(flowName + "." + i);
                            endpoint.setBatchListener(true);
                            endpoint.setBatchingStrategy(new SimpleBatchingStrategy(100, 25_000, 1));
                            endpoint.setMessageListener(dataflowReceiver);
                            endpoint.setQueueNames(i);
                            registrar.registerEndpoint(endpoint);
                        });
                    });
        }
    }


    public void initFlowContext() {
        if (DataflowContext.initialized) {
            return;
        }
        synchronized (this) {
            if (DataflowContext.initialized) {
                return;
            }
            log.info("开始初始化 Flow 上下文...");
            Map<String, Object> map = applicationContext.getBeansWithAnnotation(Flow.class);
            map.entrySet()
                    .stream()
                    .parallel()
                    .forEach(i -> {
                        String beanName = i.getKey();
                        Object bean = i.getValue();
                        var flowAnnotation = AopUtils.getTargetClass(bean)
                                .getAnnotation(Flow.class);
                        log.debug("Register Dataflow「{}」: [{}] ", dataflowConfig.getRunMode(), beanName);
                        // 设置stepTree
                        Map<String, String> parentMap = new HashMap<>();
                        String exchangeName = flowAnnotation.name();
                        if (!StringUtils.hasLength(exchangeName)) {
                            exchangeName = bean.getClass()
                                    .getName();
                        }

                        Method[] declaredMethods = bean.getClass()
                                .getDeclaredMethods();
                        String finalExchangeName = exchangeName;
                        List<String> jobNames = new ArrayList<>();
                        Stream.of(declaredMethods)
                                .forEach(m -> {
                                    Step stepAnnotation = m.getAnnotation(Step.class);
                                    // 如果存在 @Step 注解，则创建相应队列
                                    if (stepAnnotation != null) {
                                        log.debug("Register DataflowStep: [{}] ", beanName + m.getName());
                                        var parentStep = bean.getClass()
                                                                 .getName() + "." + stepAnnotation.preStep();
                                        if (StrUtil.isBlank(stepAnnotation.preStep())
                                            || "root".equalsIgnoreCase(stepAnnotation.preStep())) {
                                            parentStep = "root";
                                        }
                                        parentMap.put(bean.getClass()
                                                              .getName() + "." + m.getName(), parentStep);

                                        var queueName = finalExchangeName + "." + m.getName();
                                        jobNames.add(queueName);

                                        // 不存在队列则创建
                                        QueueInformation queueInfo = rabbitAdmin.getQueueInfo(queueName);
                                        if (queueInfo == null) {
                                            Queue stepQueue = QueueBuilder.durable(queueName)
                                                    .build();
                                            // 创建业务队列
                                            rabbitAdmin.declareQueue(stepQueue);
                                        }
                                        // routing(重试)
                                        QueueInformation routingQueueInfo = rabbitAdmin.getQueueInfo(queueName + ".routing");
                                        if (routingQueueInfo == null) {
                                            Queue routingQueue = QueueBuilder.durable(queueName + ".routing")
                                                    .deadLetterExchange("")
                                                    .deadLetterRoutingKey(queueName)
                                                    .ttl(5000)
                                                    .build();
                                            rabbitAdmin.declareQueue(routingQueue);
                                        }
                                        // fatal(最终放弃重试)
                                        QueueInformation fatalQueueInfo = rabbitAdmin.getQueueInfo(queueName + ".fatal");
                                        if (fatalQueueInfo == null) {
                                            Queue fatalQueue = QueueBuilder.durable(queueName + ".fatal")
                                                    .build();
                                            rabbitAdmin.declareQueue(fatalQueue);
                                        }
                                    }
                                });
                        List<TreeNode<String>> nodeList = CollUtil.newArrayList();
                        parentMap.forEach((k, v) -> {
                            nodeList.add(new TreeNode<>(k, v, k, 1));
                        });

                        // 加入到上下文中
                        dataflowContext.addDataFlowJobsMap(beanName, jobNames);
                        List<Tree<String>> treeList = TreeUtil.build(nodeList, "root");
                        if (CollUtil.isNotEmpty(treeList)) {
                            dataflowContext.addDataflowTree(beanName, treeList);
                        }
                    });

            DataflowContext.initialized = true;
            log.info("初始化 Flow 上下文完成。");
        }
    }
}
