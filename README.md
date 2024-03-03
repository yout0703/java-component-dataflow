# java-component-dataflow

Java 的数据流处理组件，支持基于注解的自定义处理步骤。
用于从一个入口(FlowController，通过 HTTP 方式提交数据)接收数据，分步处理/清洗数据的场景

# 1. Features

- 通过 Java 注解快速注册 Flow，使用人员不用关心底层逻辑实现。
- 支持树形结构处理流，并行处理同级别数据步骤。
- 支持分布式部署。
- 支持数据处理重试。
- 支持数据处理失败后写入到 fatal 队列，便于查看出错信息以及后续通过消息移动功能重跑失败数据。
- 支持 mq 模式和 http 模式，方便本地调试。

# 2. 使用说明

- 引入依赖：

```xml

<dependency>
    <groupId>yout.component</groupId>
    <artifactId>java-component-dataflow</artifactId>
    <version>1.0.0</version>
</dependency>
```

- 配置环境，主要是配置 RabbitMQ 的相关参数

```yaml
spring:
  application:
    name: rabbitmq-dataflow
  rabbitmq:
    host: ${RABBITMQ_HOST}
    port: ${RABBITMQ_PORT}
    username: ${RABBITMQ_USERNAME}
    password: ${RABBITMQ_PASSWORD}
    virtual-host: ${RABBITMQ_VHOST}
    listener:
      direct:
        prefetch: 10
        retry:
          enabled: true

dataflow:
  runMode: http # 这里可以控制使用哪种方式处理数据流，http 方式方便调试，生产环境使用 mq 方式。
```

- @Flow：标识整个完整的数据流。通过 @Flow 类注解可以注册一个普通的 Java 类成为一个数据流处理类。
- @Step：
    - 数据流中的其中一步操作。通过 @Step 方法注解可以注册一个普通的 Java 方法为数据流处理步骤。
    - @Step 注解支持配置 preStep 属性，来告诉 Flow 框架，在哪一步执行完后，再执行此步骤。如：@Step(preStep="odlHandle")
    - 上一步的输出可以作为下一步的输入参数

# 3. 默认约定

- 目前在处理 Flow 数据时最多会启动 30 个消费者并行消费。消费者数量根据堆积的消息自动增减。
- Flow 调用入口: 将类名中 Flow 去掉，剩下的转换为下划线。如：AaaBbbFlow ，调用入口 {{host}}/flow/aaa_bbb

# 4. 代码示例

```java

@Flow
@Slf4j
public class DemoFlow {

    @Step
    public UserDemo stepOne(UserDemo userDemo) {
        log.info("DemoFlow,stepOne start,id = {}", 1);
        return userDemo;
    }

    @Step(preStep = "stepOne")
    public UserDemo stepTwo(UserDemo userDemo) {
        log.info("DemoFlow,stepTwo start,id = {}", 1);
        return userDemo;
    }

    @Step(preStep = "stepOne")
    public UserDemo stepThree(UserDemo userDemo) {
        log.info("DemoFlow,stepThree start，id = {}", 1);
        return userDemo;
    }

    @Step(preStep = "stepOne")
    public UserDemo stepFour(UserDemo userDemo) {
        log.info("DemoFlow, stepFour start,id = {}", 1);
        return userDemo;
    }

    @Step(preStep = "stepThree")
    public void stepFive(UserDemo userDemo) {
        log.info("DemoFlow, stepFive start,id = {}", 1);
    }

    @Step(preStep = "stepFour")
    public void stepSix(UserDemo userDemo) {
        log.info("DemoFlow, stepSix start,id={}", 1);
    }
}
```





