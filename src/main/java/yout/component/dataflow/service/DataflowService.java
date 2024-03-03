package yout.component.dataflow.service;

import cn.hutool.core.util.ReflectUtil;
import cn.hutool.core.util.StrUtil;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.Resource;
import yout.component.dataflow.config.DataflowConfig;
import yout.component.dataflow.context.DataflowContext;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.context.ApplicationContext;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Optional;

/**
 * @description: 数据流服务
 * @author: yout0703
 * @date: 2023-07-28
 */
@Slf4j
@Service
public class DataflowService {

	@Resource
	private RabbitTemplate rabbitTemplate;

	@Resource
	private ApplicationContext appContext;

	@Resource
	private DataflowConfig dataflowConfig;

	@Resource
	private DataflowContext dataflowContext;

	@Resource
	private ObjectMapper objectMapper;

	@Async
	public void sendStepMessage(List<String> nextSteps, JsonNode requestJson) {
		Optional.of(nextSteps).ifPresent(steps -> steps.stream().parallel().forEach(i -> {
			rabbitTemplate.convertAndSend(i, requestJson);
			log.debug("#DATAFLOW[mq]# Message sent successfully。 Queue name is {}", i);
		}));
	}

	public LinkedHashMap<String, Object> invokeFlowStep(String currentStep, byte[] param,
	                                                    LinkedHashMap<String, Object> returnResults) {
		if (param == null || param.length == 0) {
			return null;
		}
		int index = StrUtil.lastIndexOfIgnoreCase(currentStep, ".");
		var beanClass = currentStep.substring(0, index);
		var methodName = currentStep.substring(index + 1);
		try {
			Method[] publicMethods = ReflectUtil.getPublicMethods(Class.forName(beanClass));
			if (publicMethods != null) {

				for (Method m : publicMethods) {
					if (methodName.equals(m.getName())) {
						Parameter p = m.getParameters()[0];
						Object methodParam;
						if (String.class.equals(p.getType())) {
							methodParam = new String(param);
						} else {
							methodParam = objectMapper.readValue(new String(param), p.getType());
						}
						var bean = appContext.getBean(Class.forName(beanClass));
						var result = ReflectUtil.invoke(bean, m, methodParam);
						JsonNode resultNode = objectMapper.convertValue(result, JsonNode.class);
						if (result == null) {
							// 执行结果是 null，忽略后续 job
							break;
						}
						List<String> nextSteps =
								dataflowContext
										.getNextSteps(StrUtil.lowerFirst(Class.forName(beanClass).getSimpleName()),
												currentStep);
						if (CollectionUtils.isEmpty(nextSteps)) {
							break;
						}
						String nextInput = resultNode.toString();
						if (resultNode.isTextual()) {
							nextInput = resultNode.textValue();
						}
						Message message = new Message(nextInput.getBytes(StandardCharsets.UTF_8));
						if (dataflowConfig.isMqEnv()) {
							nextSteps.stream().parallel()
									.forEach(i -> rabbitTemplate.convertAndSend(i, message));

						} else {
							nextSteps.stream().parallel().forEach(i -> invokeFlowStep(i,
									message.getBody(), returnResults));
							returnResults.put(currentStep.replace(beanClass + ".", ""), result);
						}
						break;
					}
				}
			}
		} catch (Exception e) {
			log.error(String.format("#Dataflow[%s]# invoke method %s error, %s", dataflowConfig.getRunMode(),
					currentStep, e.getMessage()), e);
			throw new RuntimeException(e);
		}
		return returnResults;
	}
}
