package net.damero.Kafka.Aspect.Components.Utility;

/**
 * No-Op implementation of MetricsRecorder.
 * Used when Micrometer is not available to avoid overhead and dependency
 * issues.
 */
public class NoOpMetricsRecorder implements MetricsRecorder {

    @Override
    public void recordSuccess(String topic, long startTime) {
        // No-Op
    }

    @Override
    public void recordFailure(String topic, Exception exception, int attempts, long startTime) {
        // No-Op
    }

    @Override
    public boolean isAvailable() {
        return false;
    }

    @Override
    public void recordBatch(String topic, int batchSize, int processed, int failed,
            long processingTimeMs, boolean fixedWindow) {
        // No-Op
    }

    @Override
    public void recordBatchWindowExpiry(String topic, long messageCount) {
        // No-Op
    }

    @Override
    public void recordBatchCapacityReached(String topic) {
        // No-Op
    }

    @Override
    public void recordBatchBackpressure(String topic) {
        // No-Op
    }
}
