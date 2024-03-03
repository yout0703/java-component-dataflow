package yout.component.dataflow.config;

import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.AcknowledgeMode;
import org.springframework.amqp.rabbit.annotation.EnableRabbit;
import org.springframework.amqp.rabbit.config.RetryInterceptorBuilder;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.retry.RejectAndDontRequeueRecoverer;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.boot.autoconfigure.amqp.RabbitProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;
import org.springframework.messaging.converter.MappingJackson2MessageConverter;
import org.springframework.retry.RetryCallback;
import org.springframework.retry.RetryContext;
import org.springframework.retry.RetryListener;
import org.springframework.retry.backoff.ExponentialBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;


/**
 * rabbitmq config
 *
 * @author yout0703
 */
@Configuration
@Slf4j
@EnableRabbit
public class RabbitmqConfig {

    @Resource
    private DataflowConfig dataflowConfig;

    @Resource
    RabbitProperties properties;

    @Bean
    public RabbitAdmin rabbitAdmin(ConnectionFactory connectionFactory) {
        return new RabbitAdmin(connectionFactory);
    }

    /**
     * 生成模板对象 因为要设置回调类，所以应是prototype类型，如果是singleton类型，则回调类为最后一次设置
     */
    @Bean
    @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    RabbitTemplate rabbitTemplate(ConnectionFactory connectionFactory) {
        CachingConnectionFactory cachingConnectionFactory = (CachingConnectionFactory) connectionFactory;
        cachingConnectionFactory.getRabbitConnectionFactory().setRequestedChannelMax(4095);
        RabbitTemplate template = new RabbitTemplate(cachingConnectionFactory);

        template.setChannelTransacted(false);
        // 序列化方式设置，主要用于发送消息时将 Object 转换为 JSON String。
        template.setMessageConverter(jackson2JsonMessageConverter());
        return template;
    }

    /**
     * 创建 Rabbit 监听容器的工厂方法，这里设置的值都会是默认值，创建后可以对容器进行复制覆盖默认值。
     *
     * @param connectionFactory rabbit 连接
     * @return 工厂方法
     */
    @Bean
    SimpleRabbitListenerContainerFactory dataflowDefaultContainerFactory(ConnectionFactory connectionFactory) {
        SimpleRabbitListenerContainerFactory factory = new SimpleRabbitListenerContainerFactory();
        // 一次性拿几个
        factory.setPrefetchCount(100);
        // 设置并发
        factory.setConcurrentConsumers(1);
        // 最大并发
        factory.setMaxConcurrentConsumers(30);
        // 设置ACK模式，AUTO：这是由 Spring-Rabbit 框架干的，如果抛出异常就 nack，否则自动 ack，注意：使用这个参数需要设置 setDefaultRequeueRejected 为
        // false，不然一直返回队列循环错误。
        factory.setAcknowledgeMode(AcknowledgeMode.MANUAL);
        // factory.setDefaultRequeueRejected(false);
        // 设置超时时间 - 30 secondsRabbitListenerConfigurer
        factory.setReceiveTimeout(30000L);
        // 设置 MQ 连接
        factory.setConnectionFactory(connectionFactory);

        // 设置 任务并发执行
        // factory.setTaskExecutor();

        // factory.setMessageListener(dataflowReceiver);

        // 非MQ环境全部不启动监听
        if (!"mq".equalsIgnoreCase(dataflowConfig.getRunMode())) {
            factory.setAutoStartup(false);
        }

        // 设置消息处理的序列化工具
        factory.setMessageConverter(jackson2JsonMessageConverter());

        // 设置消息处理前后的逻辑，如：消息失败重试
        factory.setAdviceChain(RetryInterceptorBuilder.stateless().recoverer(new RejectAndDontRequeueRecoverer())
                .retryOperations(rabbitRetryTemplate()).build());

        return factory;
    }

    /**
     * 这个重试模板是当任务执行出问题的时候的重试模板，一般任务正常失败会进入 routing 队列不会使用这个模板。
     *
     * @return
     */
    @Bean
    public RetryTemplate rabbitRetryTemplate() {
        RetryTemplate retryTemplate = new RetryTemplate();

        // 设置监听（不是必须）
        retryTemplate.registerListener(new RetryListener() {
            @Override
            public <T, E extends Throwable> boolean open(RetryContext retryContext, RetryCallback<T, E> retryCallback) {
                return true;
            }

            @Override
            public <T, E extends Throwable> void close(RetryContext retryContext, RetryCallback<T, E> retryCallback,
                                                       Throwable throwable) {
                // 重试结束的时候调用 （最后一次重试 ）
            }

            @Override
            public <T, E extends Throwable> void onError(RetryContext retryContext, RetryCallback<T, E> retryCallback,
                                                         Throwable throwable) {
                // 异常 都会调用
                log.error(String.format("[rabbitmq-consumer-retry] 第%d次重试,%s", retryContext.getRetryCount(),
                        throwable.getMessage()), throwable);
            }
        });

        retryTemplate.setBackOffPolicy(backOffPolicyByProperties());
        retryTemplate.setRetryPolicy(retryPolicyByProperties());
        return retryTemplate;
    }

    @Bean
    public ExponentialBackOffPolicy backOffPolicyByProperties() {
        ExponentialBackOffPolicy backOffPolicy = new ExponentialBackOffPolicy();
        long maxInterval = properties.getListener().getSimple().getRetry().getMaxInterval().getSeconds();
        long initialInterval = properties.getListener().getSimple().getRetry().getInitialInterval().getSeconds();
        double multiplier = properties.getListener().getSimple().getRetry().getMultiplier();
        // 重试间隔
        backOffPolicy.setInitialInterval(initialInterval * 1000);
        // 重试最大间隔
        backOffPolicy.setMaxInterval(maxInterval * 1000);
        // 重试间隔乘法策略
        backOffPolicy.setMultiplier(multiplier);
        return backOffPolicy;
    }

    @Bean
    public SimpleRetryPolicy retryPolicyByProperties() {
        SimpleRetryPolicy retryPolicy = new SimpleRetryPolicy();
        int maxAttempts = properties.getListener().getSimple().getRetry().getMaxAttempts();
        retryPolicy.setMaxAttempts(maxAttempts);
        return retryPolicy;
    }

    @Bean
    public MessageConverter jackson2JsonMessageConverter() {
        return new Jackson2JsonMessageConverter();
    }

    @Bean
    public MappingJackson2MessageConverter mappingJackson2MessageConverter() {
        return new MappingJackson2MessageConverter();
    }
}
