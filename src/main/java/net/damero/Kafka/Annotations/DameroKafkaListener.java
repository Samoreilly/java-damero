package net.damero.Kafka.Annotations;

import net.damero.Kafka.Config.DelayMethod;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation to enhance Kafka listeners with automatic retry logic, DLQ
 * routing,
 * circuit breaker support, deduplication, and distributed tracing.
 *
 * Must be used alongside Spring's {@code @KafkaListener} annotation.
 *
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface DameroKafkaListener {

    /** The Kafka topic this listener consumes from. Required. */
    String topic();

    /**
     * The dead letter queue topic for failed messages. If empty, DLQ is disabled.
     */
    String dlqTopic() default "dlq";

    /** Maximum retry attempts before sending to DLQ. Default: 3 */
    int maxAttempts() default 3;

    /**
     * Base delay in milliseconds for retry backoff calculation. Default: 0 (no
     * delay)
     */
    double delay() default 0.0;

    /**
     * Backoff strategy for retry delays.
     * EXPO: exponential (delay * 2^attempt), LINEAR: linear (delay * attempt),
     * FIBONACCI: fibonacci sequence, CUSTOM/MAX: fixed delay value.
     */
    DelayMethod delayMethod() default DelayMethod.EXPO;

    /** Maximum fibonacci sequence index for FIBONACCI delay method. Default: 15 */
    int fibonacciLimit() default 15;

    /**
     * Whether to retry failed messages. Set to false to send directly to DLQ on
     * first failure.
     */
    boolean retryable() default true;

    /** Topic for retryable messages (currently unused, reserved for future use). */
    String retryableTopic() default "retryable-topic";

    /**
     * Exception types that should NOT be retried and go directly to DLQ.
     * Useful for validation errors or business logic failures that won't succeed on
     * retry.
     */
    Class<? extends Throwable>[] nonRetryableExceptions() default {};

    /**
     * Conditional DLQ routes for specific exception types.
     * Allows routing different exceptions to different DLQ topics.
     */
    DlqExceptionRoutes[] dlqRoutes() default {};

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

    /**
     * Time window in milliseconds for tracking failures. Default: 60000 (1 minute)
     */
    long circuitBreakerWindowDuration() default 60000;

    /**
     * Time in milliseconds to wait before attempting half-open. Default: 60000 (1
     * minute)
     */
    long circuitBreakerWaitDuration() default 60000;

    /**
     * Enable message deduplication to prevent duplicate message processing.
     * Uses Redis (distributed) or Caffeine (local) cache depending on
     * configuration.
     * Requires eventId field in your event object.
     */
    boolean deDuplication() default false;

    /**
     * Enable OpenTelemetry distributed tracing for this listener.
     * Requires opentelemetry-api on classpath. Traces include retry attempts,
     * DLQ routing, circuit breaker state, and processing duration.
     * Set to false for high-volume topics where tracing overhead is unacceptable.
     */
    boolean openTelemetry() default false;

    /**
     * Batch processing configuration. Set batchCapacity > 0 to enable.
     *
     * Two batch modes available (controlled by fixedWindow):
     *
     * Mode 1: Capacity-First (fixedWindow = false, default)
     *
     * Batch processes IMMEDIATELY when batchCapacity is reached
     * batchWindowLength is a fallback timer for slow periods
     * Use case: Maximum throughput, process as fast as possible
     *
     *
     * Mode 2: Fixed Window (fixedWindow = true)
     *
     * Batch processes ONLY when the window timer expires
     * batchCapacity is the maximum messages collected per window
     * Use case: Predictable intervals, rate-controlled processing
     *
     *
     * IT'S RECOMMENDED TO NOT MIX BATCH PROCESSING WITH
     * messagesPerWindow/messageWindow
     */

    /** Maximum messages per batch. Set to 0 to disable batch processing. */
    int batchCapacity() default 0;

    /**
     * Minimum messages to trigger early processing (only in capacity-first mode).
     */
    int minimumCapacity() default 0;

    /** Batch window duration in milliseconds. Default: 2500ms (2.5 seconds) */
    int batchWindowLength() default 2500;

    /**
     * Enable fixed window mode for predictable batch timing.
     *
     * false: Process immediately when capacity reached, window is fallback
     * true: Process ONLY when window expires, capacity is just a limit
     *
     */
    boolean fixedWindow() default false;

}
