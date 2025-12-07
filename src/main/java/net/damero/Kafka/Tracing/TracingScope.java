package net.damero.Kafka.Tracing;

/**
 * Wrapper interface for trace scope that hides the underlying tracing implementation.
 *
 * A scope represents the time period during which a span is the "current" span.
 * This must be closed when the scope ends (use try-with-resources).
 *
 * Example usage:
 * <pre>
 * TracingSpan span = tracingService.startProcessingSpan(...);
 * try (TracingScope scope = span.makeCurrent()) {
 *     // do work - span is now the current span
 * } finally {
 *     span.end();
 * }
 * </pre>
 */
public interface TracingScope extends AutoCloseable {

    /**
     * Closes the scope. This restores the previous span as the current span.
     * Unlike AutoCloseable.close(), this method does not throw exceptions.
     */
    @Override
    void close();
}

