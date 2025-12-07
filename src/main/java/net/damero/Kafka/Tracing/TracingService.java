package net.damero.Kafka.Tracing;

import org.apache.kafka.common.header.Headers;
import org.springframework.lang.Nullable;

/**
 * Interface for tracing operations in the Damero library.
 *
 * This interface allows OpenTelemetry to be an optional dependency.
 * When OpenTelemetry is on the classpath, the real implementation (OpenTelemetryTracingService)
 * is used. When OpenTelemetry is absent, a no-op implementation (NoOpTracingService) is used
 * that does nothing but allows the application to run without errors.
 *
 * All span objects are wrapped in TracingSpan to avoid exposing OpenTelemetry types
 * in the interface, which would cause ClassNotFoundException when OpenTelemetry is absent.
 */
public interface TracingService {

    /**
     * Creates a new span for message processing.
     *
     * @param operationName the name of the operation
     * @param topic the Kafka topic
     * @param eventId the event ID (optional)
     * @return a TracingSpan wrapper (never null, but may be a no-op)
     */
    TracingSpan startProcessingSpan(String operationName, String topic, @Nullable String eventId);

    /**
     * Creates a new span for retry operations.
     *
     * @param topic the Kafka topic
     * @param eventId the event ID (optional)
     * @param attempt the current retry attempt
     * @param delayMs the retry delay in milliseconds
     * @return a TracingSpan wrapper
     */
    TracingSpan startRetrySpan(String topic, @Nullable String eventId, int attempt, long delayMs);

    /**
     * Creates a new span for DLQ routing operations.
     *
     * @param originalTopic the original Kafka topic
     * @param dlqTopic the DLQ topic
     * @param eventId the event ID (optional)
     * @param attempts the number of attempts made
     * @param reason the reason for DLQ routing
     * @return a TracingSpan wrapper
     */
    TracingSpan startDLQSpan(String originalTopic, String dlqTopic, @Nullable String eventId,
                             int attempts, String reason);

    /**
     * Creates a new span for circuit breaker operations.
     *
     * @param topic the Kafka topic
     * @param state the circuit breaker state
     * @return a TracingSpan wrapper
     */
    TracingSpan startCircuitBreakerSpan(String topic, String state);

    /**
     * Creates a new span for deduplication checks.
     *
     * @param eventId the event ID
     * @param isDuplicate whether the message is a duplicate
     * @return a TracingSpan wrapper
     */
    TracingSpan startDeduplicationSpan(String eventId, boolean isDuplicate);

    /**
     * Creates a new span for rate limiting operations.
     *
     * @param topic the Kafka topic
     * @param currentCount the current message count in window
     * @param maxMessages the maximum messages allowed per window
     * @param sleepMs the sleep time in milliseconds (0 if not rate limited)
     * @return a TracingSpan wrapper
     */
    TracingSpan startRateLimitSpan(String topic, int currentCount, int maxMessages, long sleepMs);

    /**
     * Creates a new span for DLQ replay operations.
     *
     * @param dlqTopic the DLQ topic being replayed
     * @return a TracingSpan wrapper
     */
    TracingSpan startReplaySpan(String dlqTopic);

    /**
     * Extracts the trace context from Kafka headers.
     *
     * @param headers the Kafka headers
     * @return a TracingContext wrapper that can be used to create child spans
     */
    TracingContext extractContext(Headers headers);

    /**
     * Injects the current trace context into Kafka headers.
     *
     * @param headers the Kafka headers to inject into
     * @param context the context to inject (if null, uses current context)
     */
    void injectContext(Headers headers, @Nullable TracingContext context);

    /**
     * Wraps a runnable with the current span context.
     *
     * @param span the span to use as context
     * @param runnable the runnable to wrap
     * @return a wrapped runnable that runs in the span's context
     */
    Runnable wrapWithContext(TracingSpan span, Runnable runnable);

    /**
     * Checks if tracing is enabled (i.e., OpenTelemetry is available).
     *
     * @return true if tracing is enabled, false for no-op implementation
     */
    boolean isEnabled();
}

