package net.damero.Kafka.Annotations;

import net.damero.Kafka.Config.DelayMethod;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation to enhance Kafka listeners with automatic retry logic, DLQ routing,
 * circuit breaker support, rate limiting, deduplication, and distributed tracing.
 *
 * <p>Must be used alongside Spring's {@code @KafkaListener} annotation.</p>
 *
 * <p>Example usage:</p>
 * <pre>
 * &#64;CustomKafkaListener(
 *     topic = "orders",
 *     dlqTopic = "orders-dlq",
 *     maxAttempts = 3,
 *     delay = 1000,
 *     delayMethod = DelayMethod.EXPO,
 *     nonRetryableExceptions = {ValidationException.class}
 * )
 * &#64;KafkaListener(topics = "orders", groupId = "order-processor")
 * public void process(OrderEvent event, Acknowledgment ack) { ... }
 * </pre>
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface CustomKafkaListener {

    /** The Kafka topic this listener consumes from. Required. */
    String topic();

    /** The dead letter queue topic for failed messages. If empty, DLQ is disabled. */
    String dlqTopic() default "";

    /** Maximum retry attempts before sending to DLQ. Default: 3 */
    int maxAttempts() default 3;

    /** Base delay in milliseconds for retry backoff calculation. Default: 0 (no delay) */
    double delay() default 0.0;

    /**
     * Backoff strategy for retry delays.
     * EXPO: exponential (delay * 2^attempt), LINEAR: linear (delay * attempt),
     * FIBONACCI: fibonacci sequence, CUSTOM/MAX: fixed delay value.
     */
    DelayMethod delayMethod() default DelayMethod.EXPO;

    /** Maximum fibonacci sequence index for FIBONACCI delay method. Default: 15 */
    int fibonacciLimit() default 15;

    /** Whether to retry failed messages. Set to false to send directly to DLQ on first failure. */
    boolean retryable() default true;

    /** Topic for retryable messages (currently unused, reserved for future use). */
    String retryableTopic() default "retryable-topic";

    /**
     * Exception types that should NOT be retried and go directly to DLQ.
     * Useful for validation errors or business logic failures that won't succeed on retry.
     */
    Class<? extends Throwable>[] nonRetryableExceptions() default {};

    /**
     * Conditional DLQ routes for specific exception types.
     * Allows routing different exceptions to different DLQ topics.
     */
    DlqExceptionRoutes[] dlqRoutes() default {};

    /** Maximum messages to process per window. Set to 0 to disable rate limiting. */
    int messagesPerWindow() default 0;

    /** Rate limiting window duration in milliseconds. Set to 0 to disable rate limiting. */
    long messageWindow() default 0;

    /** Custom KafkaTemplate bean class to use. Default: uses default template. */
    Class<Void> kafkaTemplate() default void.class;

    /** Custom ConsumerFactory bean class to use. Default: uses default factory. */
    Class<Void> consumerFactory() default void.class;

    /** Event type class for deserialization. Default: auto-detected. */
    Class<?> eventType() default Void.class;

    /** Enable circuit breaker to stop processing when failure rate is too high. */
    boolean enableCircuitBreaker() default false;

    /** Number of failures in window before circuit opens. Default: 50 */
    int circuitBreakerFailureThreshold() default 50;

    /** Time window in milliseconds for tracking failures. Default: 60000 (1 minute) */
    long circuitBreakerWindowDuration() default 60000;

    /** Time in milliseconds to wait before attempting half-open. Default: 60000 (1 minute) */
    long circuitBreakerWaitDuration() default 60000;

    /**
     * Enable message deduplication to prevent duplicate message processing.
     * Uses Redis (distributed) or Caffeine (local) cache depending on configuration.
     * Requires eventId field in your event object.
     */
    boolean deDuplication() default false;

    /**
     * Enable OpenTelemetry distributed tracing for this listener.
     * Requires opentelemetry-api on classpath. Traces include retry attempts,
     * DLQ routing, circuit breaker state, and rate limiting events.
     * Set to false for high-volume topics where tracing overhead is unacceptable.
     */
    boolean openTelemetry() default false;
}

