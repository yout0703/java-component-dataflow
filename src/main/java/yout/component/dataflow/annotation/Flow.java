package yout.component.dataflow.annotation;

import java.lang.annotation.*;

import org.springframework.core.annotation.AliasFor;
import org.springframework.stereotype.Component;

/**
 * @description: 数据流标识注解，标识该注解会被框架解析为数据流处理类
 * @author: yout0703
 * @date: 2023-07-03
 */
@Documented
@Inherited
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE})
@Component
public @interface Flow {
    /**
     * 可以自定义队列名
     */
    String value() default "";

    @AliasFor("value")
    String name() default "";
}
