package net.damero.Annotations;

import net.damero.CustomKafkaSetup.DelayMethod;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface CustomKafkaListener {

    String topic();
    String dlqTopic() default "";
    int maxAttempts() default 3;
    double delay() default 0.0;
    DelayMethod delayMethod() default DelayMethod.EXPO;
    Class<Void> kafkaTemplate() default void.class;
    Class<Void> consumerFactory() default void.class;
    Class<?> eventType() default Void.class;
}
