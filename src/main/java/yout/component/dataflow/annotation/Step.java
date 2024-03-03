package yout.component.dataflow.annotation;

import java.lang.annotation.*;

import org.springframework.core.annotation.AliasFor;

/**
 * @description: 数据流的步骤，组成数据流的每一步
 * @author: yout0703
 * @date: 2023-07-03
 */
@Documented
@Inherited
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD})
public @interface Step {

    String value() default "";

    /**
     * 当前步骤的上一步是哪个
     *
     * @return
     */
    @AliasFor("value")
    String preStep() default "";
}
