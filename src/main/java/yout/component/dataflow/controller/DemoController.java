package yout.component.dataflow.controller;

import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import yout.component.dataflow.config.UserDemo;

/**
 * @description:
 * @author: yout0703
 * @date: 2023-07-11
 */
@RestController
public class DemoController {

    @Autowired
    private RabbitTemplate rabbitTemplate;

    @PostMapping("/demo/send")
    public String send(@RequestBody UserDemo userDemo) {
        try {
            rabbitTemplate.convertAndSend("test.DemoFlow.step1", userDemo);
        } catch (Exception e) {
            return e.getMessage();
        }
        return "success";
    }
}
