package yout.component.dataflow.config;

import java.io.Serializable;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @description:
 * @author: yout0703
 * @date: 2023-07-05
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class UserDemo implements Serializable {
    private String name;
    private Integer age;
}
