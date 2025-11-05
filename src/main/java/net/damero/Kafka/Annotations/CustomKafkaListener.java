package net.damero.Kafka.Annotations;

import net.damero.Kafka.Config.DelayMethod;

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
    boolean retryable() default true;
    String retryableTopic() default "retryable-topic";
    Class<? extends Throwable>[] nonRetryableExceptions() default {};
    int messagesPerWindow() default 0;// amount of messages to process per window
    long messageWindow() default 0;//window duration in milliseconds
    Class<Void> kafkaTemplate() default void.class;
    Class<Void> consumerFactory() default void.class;
    Class<?> eventType() default Void.class;
    
    // Circuit Breaker Configuration
    boolean enableCircuitBreaker() default false;
    int circuitBreakerFailureThreshold() default 50;  // Number of failures before opening (opening is bad)
    long circuitBreakerWindowDuration() default 60000;  // Time window in milliseconds (default 1 minute)
    long circuitBreakerWaitDuration() default 60000;  // Wait before half-open (default 1 minute)
}
