package yout.component.dataflow.controller;

import cn.hutool.core.util.StrUtil;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.context.ApplicationContext;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import yout.component.dataflow.config.DataflowConfig;
import yout.component.dataflow.context.DataflowContext;
import yout.component.dataflow.service.DataflowService;

import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * @description: 数据流入口
 * @author: yout0703
 * @date: 2023-07-03
 */
@RestController
@Slf4j
public record DataflowController(RabbitTemplate rabbitTemplate, ObjectMapper objectMapper,
                                 DataflowContext dataflowContext, ApplicationContext applicationContext,
                                 DataflowConfig dataflowConfig,
                                 DataflowService dataflowService) {


    @PostMapping("/flow/{flow_name}")
    public Map<String, Object> sendDirectMessage(@PathVariable("flow_name") String flowName,
                                                 @RequestBody JsonNode requestJson) {
        log.debug(
                "#DATAFLOW[{}]# The Flow message was received，current dataflow env:[{}]。 flow_name: [{}], message : {}",
                dataflowConfig.getRunMode(), dataflowConfig.getRunMode(), flowName, requestJson.toString()
                        .substring(0, Math.min(requestJson.toString()
                                .length(), 20)));
        try {
            flowName = StrUtil.toCamelCase(flowName) + "Flow";
            List<String> nextSteps = dataflowContext.getEntranceSteps(flowName);
            if (nextSteps.isEmpty()) {
                log.error("nextSteps size 为0 请检查flowName {}", flowName);
                return Map.of("fail", "nextSteps size 为0 请检查flowName");
            }

            // 只有mq环境才去异步发消息
            if (dataflowConfig.isMqEnv()) {
                dataflowService.sendStepMessage(nextSteps, requestJson);
                return Map.of("success", "true");
            } else { // 其他环境直接执行stepTree
                LinkedHashMap<String, Object> returnResults = new LinkedHashMap<>();
                Optional.of(nextSteps)
                        .ifPresent(steps -> steps.stream()
                                .parallel()
                                .forEach(i -> {
                                    var result = dataflowService.invokeFlowStep(i,
                                            requestJson.toString()
                                                    .getBytes(StandardCharsets.UTF_8), returnResults);
                                    log.debug("#DATAFLOW[http]# Invoke method successfully。 Method name is {}", i);
                                }));
                return returnResults;
            }
        } catch (Exception e) {
            log.error("#DATAFLOW[{}]# Invoke method failed。 Method name is {}", dataflowConfig.getRunMode(), flowName);
            return Map.of("fail", e.getMessage());
        }
    }
}
