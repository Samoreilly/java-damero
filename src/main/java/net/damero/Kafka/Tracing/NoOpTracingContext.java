package net.damero.Kafka.Tracing;

import org.springframework.lang.Nullable;

/**
 * No-op implementation of TracingContext used when OpenTelemetry is not available.
 */
public class NoOpTracingContext implements TracingContext {

    /** Singleton instance to avoid creating many no-op objects */
    public static final NoOpTracingContext INSTANCE = new NoOpTracingContext();

    private NoOpTracingContext() {
        // Private constructor for singleton
    }

    @Override
    public TracingScope with(TracingSpan span) {
        return NoOpTracingScope.INSTANCE;
    }

    @Override
    @Nullable
    public Object getUnderlyingContext() {
        return null;
    }
}

