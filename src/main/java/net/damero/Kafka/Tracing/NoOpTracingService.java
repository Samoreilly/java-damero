package net.damero.Kafka.Tracing;

import org.apache.kafka.common.header.Headers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.lang.Nullable;

/**
 * No-op implementation of TracingService used when OpenTelemetry is not available.
 *
 * This implementation does nothing but allows the library to function without
 * OpenTelemetry dependencies. All span creation methods return no-op spans
 * that silently ignore all operations.
 *
 * This class is automatically used when OpenTelemetry is not on the classpath.
 */
public class NoOpTracingService implements TracingService {

    private static final Logger logger = LoggerFactory.getLogger(NoOpTracingService.class);

    public NoOpTracingService() {
        logger.info("OpenTelemetry not available - tracing disabled. " +
                   "Add opentelemetry-api dependency to enable distributed tracing.");
    }

    @Override
    public TracingSpan startProcessingSpan(String operationName, String topic, @Nullable String eventId) {
        return NoOpTracingSpan.INSTANCE;
    }

    @Override
    public TracingSpan startRetrySpan(String topic, @Nullable String eventId, int attempt, long delayMs) {
        return NoOpTracingSpan.INSTANCE;
    }

    @Override
    public TracingSpan startDLQSpan(String originalTopic, String dlqTopic, @Nullable String eventId,
                                    int attempts, String reason) {
        return NoOpTracingSpan.INSTANCE;
    }

    @Override
    public TracingSpan startCircuitBreakerSpan(String topic, String state) {
        return NoOpTracingSpan.INSTANCE;
    }

    @Override
    public TracingSpan startDeduplicationSpan(String eventId, boolean isDuplicate) {
        return NoOpTracingSpan.INSTANCE;
    }

    @Override
    public TracingSpan startRateLimitSpan(String topic, int currentCount, int maxMessages, long sleepMs) {
        return NoOpTracingSpan.INSTANCE;
    }

    @Override
    public TracingSpan startReplaySpan(String dlqTopic) {
        return NoOpTracingSpan.INSTANCE;
    }

    @Override
    public TracingContext extractContext(Headers headers) {
        return NoOpTracingContext.INSTANCE;
    }

    @Override
    public void injectContext(Headers headers, @Nullable TracingContext context) {
        // No-op - nothing to inject when tracing is disabled
    }

    @Override
    public Runnable wrapWithContext(TracingSpan span, Runnable runnable) {
        // Just return the runnable as-is since we have no context to wrap
        return runnable;
    }

    @Override
    public boolean isEnabled() {
        return false;
    }
}

