package net.damero.Kafka.Tracing;

import org.springframework.lang.Nullable;

/**
 * No-op implementation of TracingSpan used when OpenTelemetry is not available.
 * 
 * All methods do nothing but return 'this' for method chaining compatibility.
 * This allows the library to function without OpenTelemetry dependencies.
 */
public class NoOpTracingSpan implements TracingSpan {

    /** Singleton instance to avoid creating many no-op objects */
    public static final NoOpTracingSpan INSTANCE = new NoOpTracingSpan();

    private NoOpTracingSpan() {
        // Private constructor for singleton
    }

    @Override
    public TracingSpan setAttribute(String key, String value) {
        return this;
    }

    @Override
    public TracingSpan setAttribute(String key, long value) {
        return this;
    }

    @Override
    public TracingSpan setAttribute(String key, boolean value) {
        return this;
    }

    @Override
    public TracingSpan setAttribute(String key, int value) {
        return this;
    }

    @Override
    public TracingSpan recordException(Throwable exception) {
        return this;
    }

    @Override
    public TracingSpan setSuccess() {
        return this;
    }

    @Override
    public TracingSpan setError(String message) {
        return this;
    }

    @Override
    public void end() {
        // No-op
    }

    @Override
    public TracingScope makeCurrent() {
        return NoOpTracingScope.INSTANCE;
    }

    @Override
    @Nullable
    public Object getUnderlyingSpan() {
        return null;
    }
}

