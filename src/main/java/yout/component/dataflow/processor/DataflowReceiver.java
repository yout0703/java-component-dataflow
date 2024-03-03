package yout.component.dataflow.processor;

import cn.hutool.core.exceptions.ExceptionUtil;
import com.rabbitmq.client.Channel;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.api.ChannelAwareMessageListener;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import yout.component.dataflow.config.DataflowConfig;
import yout.component.dataflow.service.DataflowService;

import java.io.IOException;

/**
 * @description: Dataflow接收消息入口
 * @author: yout0703
 * @date: 2023-07-08
 */
@Component
@Slf4j
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE) // 每个线程使用不同的实例防止阻塞
public class DataflowReceiver implements ChannelAwareMessageListener {

    private final DataflowService dataflowService;

    private final DataflowConfig dataflowConfig;

    private final RabbitTemplate rabbitTemplate;

    public DataflowReceiver(DataflowService dataflowService, DataflowConfig dataflowConfig, RabbitTemplate rabbitTemplate) {
        this.dataflowService = dataflowService;
        this.dataflowConfig = dataflowConfig;
        this.rabbitTemplate = rabbitTemplate;
    }

    @Override
    public void onMessage(Message message, Channel channel) {
        log.debug("#DATAFLOW-CONSUMER# Dataflow message handler received message: {}", message);
        long deliveryTag = message.getMessageProperties().getDeliveryTag();
        var queueName = message.getMessageProperties().getConsumerQueue();
        Integer retryCount = message.getMessageProperties().getHeader("retry_count");
        if (retryCount == null) {
            retryCount = 0;
        }
        try {
            if (retryCount > 1) {
                log.debug("Retry time #{}, queue name = {},message = {}", retryCount - 1, queueName, message);
            }
            // 操作重试次数直接进入 fatal 队列
            if (retryCount > dataflowConfig.getMaxRetryCount()) {
                // 重新设置为0
                message.getMessageProperties().setHeader("retry_count", 0);
                rabbitTemplate.convertAndSend(queueName + ".fatal", message);
                log.warn(
                        "The maximum retry count has been exceeded,send to the fatal queue. queue name = {},message = {}",
                        queueName, message);
                return;
            }
            dataflowService.invokeFlowStep(queueName, message.getBody(), null);
        } catch (Exception e) {
            message.getMessageProperties().setHeader("retry_count", retryCount + 1);
            message.getMessageProperties().setHeader("exception", ExceptionUtil.getRootCauseMessage(e));
            rabbitTemplate.convertAndSend(queueName + ".routing", message);
            log.debug("Dataflow handle message[{}] error, send to routing queue. {}", message, e.getMessage());
        } finally {
            try {
                channel.basicAck(deliveryTag, false);
            } catch (IOException e) {
                log.error("Dataflow handle message[{}] error, ack failed. {}", message, e.getMessage());
            }
        }
    }


}
