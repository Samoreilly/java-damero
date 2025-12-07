package net.damero.Kafka.Tracing;

/**
 * No-op implementation of TracingScope used when OpenTelemetry is not available.
 *
 * The close() method does nothing.
 */
public class NoOpTracingScope implements TracingScope {

    /** Singleton instance to avoid creating many no-op objects */
    public static final NoOpTracingScope INSTANCE = new NoOpTracingScope();

    private NoOpTracingScope() {
        // Private constructor for singleton
    }

    @Override
    public void close() {
        // No-op
    }
}

