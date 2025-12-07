package net.damero.Kafka.Tracing;

import org.springframework.lang.Nullable;

/**
 * Wrapper interface for trace spans that hides the underlying tracing implementation.
 * 
 * This abstraction allows the library to work with or without OpenTelemetry.
 * When OpenTelemetry is present, this wraps a real Span.
 * When OpenTelemetry is absent, this is a no-op implementation.
 * 
 * Users should not need to interact with this class directly - it's used internally
 * by the library to manage trace spans.
 */
public interface TracingSpan {

    /**
     * Sets an attribute on the span.
     * 
     * @param key the attribute key
     * @param value the attribute value
     * @return this span for chaining
     */
    TracingSpan setAttribute(String key, String value);

    /**
     * Sets an attribute on the span.
     * 
     * @param key the attribute key
     * @param value the attribute value
     * @return this span for chaining
     */
    TracingSpan setAttribute(String key, long value);

    /**
     * Sets an attribute on the span.
     * 
     * @param key the attribute key
     * @param value the attribute value
     * @return this span for chaining
     */
    TracingSpan setAttribute(String key, boolean value);

    /**
     * Sets an attribute on the span.
     * 
     * @param key the attribute key
     * @param value the attribute value
     * @return this span for chaining
     */
    TracingSpan setAttribute(String key, int value);

    /**
     * Records an exception in the span.
     * 
     * @param exception the exception to record
     * @return this span for chaining
     */
    TracingSpan recordException(Throwable exception);

    /**
     * Sets the span status to success/OK.
     * 
     * @return this span for chaining
     */
    TracingSpan setSuccess();

    /**
     * Sets the span status to error with a message.
     * 
     * @param message the error message
     * @return this span for chaining
     */
    TracingSpan setError(String message);

    /**
     * Ends the span. Must be called when the operation is complete.
     */
    void end();

    /**
     * Makes this span the current span in the context.
     * Returns a scope that must be closed when done.
     * 
     * @return a TracingScope that must be closed (use try-with-resources)
     */
    TracingScope makeCurrent();

    /**
     * Gets the underlying span object if needed for advanced operations.
     * Returns null for no-op implementations.
     * 
     * @return the underlying span object, or null
     */
    @Nullable
    Object getUnderlyingSpan();
}

