package java.damero.CustomKafkaSetup;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface CustomKafka {
    String topic() default "";
    String dlqTopic() default "";
    int maxAttempts() default 3;
    boolean customRetry() default false;
}
