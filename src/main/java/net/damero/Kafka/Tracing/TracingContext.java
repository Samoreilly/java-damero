package net.damero.Kafka.Tracing;

import org.springframework.lang.Nullable;

/**
 * Wrapper interface for trace context that hides the underlying tracing implementation.
 *
 * A context carries trace information (trace ID, span ID, etc.) that can be
 * propagated across service boundaries via Kafka headers.
 *
 * This abstraction allows the library to work with or without OpenTelemetry.
 */
public interface TracingContext {

    /**
     * Creates a child span from this context.
     * The new span will be a child of the span represented by this context.
     *
     * @param span the span to associate with this context
     * @return a TracingScope that must be closed
     */
    TracingScope with(TracingSpan span);

    /**
     * Gets the underlying context object if needed for advanced operations.
     * Returns null for no-op implementations.
     *
     * @return the underlying context object, or null
     */
    @Nullable
    Object getUnderlyingContext();
}

