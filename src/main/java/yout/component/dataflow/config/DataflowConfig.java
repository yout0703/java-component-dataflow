package yout.component.dataflow.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableAsync;

/**
 * @description: 数据流配置
 * @author: yout0703
 * @date: 2023-07-06
 */
@Configuration
@ConfigurationProperties(prefix = "dataflow")
@Data
@EnableAsync
public class DataflowConfig {

    public static final String MQ_RUN_MODE = "mq";

    private String runMode;
    private Integer maxRetryCount = 3;

    public boolean isMqEnv() {
        return MQ_RUN_MODE.equalsIgnoreCase(runMode);
    }
}
