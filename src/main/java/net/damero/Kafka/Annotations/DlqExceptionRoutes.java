package net.damero.Kafka.Annotations;

/**
 * Defines a conditional DLQ route for specific exception types.
 * Allows different exceptions to be routed to different DLQ topics.
 *
 * Example:
 * <pre>
 * @CustomKafkaListener(
 *     topic = "orders",
 *     dlqRoutes = {
 *         @DlqExceptionRoutes(
 *             exception = {ValidationException.class},
 *             dlqExceptionTopic = "orders-validation",
 *             skipRetry = true  // Send directly to DLQ, no retries
 *         ),
 *         @DlqExceptionRoutes(
 *             exception = {TimeoutException.class},
 *             dlqExceptionTopic = "orders-timeout",
 *             skipRetry = false  // Retry first, then send to DLQ
 *         )
 *     }
 * )
 * </pre>
 */
public @interface DlqExceptionRoutes {

    /**
     * The exception classes that should be routed to this DLQ.
     * Supports inheritance - subclasses will also match.
     */
    Class<? extends Throwable> exception();

    /**
     * The target DLQ topic for these exceptions.
     */
    String dlqExceptionTopic() default "";

    /**
     * Whether to skip retry logic and send directly to DLQ.
     *
     * true = Send immediately to conditional DLQ (recommended for validation/business errors)
     * false = Retry first, then send to conditional DLQ after max attempts (for transient errors)
     *
     * Default: true (no retries for routed exceptions)
     */
    boolean skipRetry() default true;
}
