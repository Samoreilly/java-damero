package net.damero.Kafka.Aspect.Components.Utility;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Micrometer-based implementation of MetricsRecorder.
 */
public class MicrometerMetricsRecorder implements MetricsRecorder {

    private static final Logger logger = LoggerFactory.getLogger(MicrometerMetricsRecorder.class);
    private final MeterRegistry meterRegistry;

    public MicrometerMetricsRecorder(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
    }

    @Override
    public void recordSuccess(String topic, long startTime) {
        try {
            Timer.Sample sample = Timer.start(meterRegistry);
            sample.stop(Timer.builder("kafka.damero.processing.time")
                    .tag("topic", topic)
                    .tag("status", "success")
                    .register(meterRegistry));

            Counter.builder("kafka.damero.processing.count")
                    .tag("topic", topic)
                    .tag("status", "success")
                    .register(meterRegistry)
                    .increment();
        } catch (Exception e) {
            logger.warn("failed to record success metrics for topic {}: {}", topic, e.getMessage());
        }
    }

    @Override
    public void recordFailure(String topic, Exception exception, int attempts, long startTime) {
        try {
            String exceptionType = exception.getClass().getSimpleName();

            Timer.builder("kafka.damero.processing.time")
                    .tag("topic", topic)
                    .tag("status", "failure")
                    .register(meterRegistry)
                    .record(System.currentTimeMillis() - startTime, java.util.concurrent.TimeUnit.MILLISECONDS);

            Counter.builder("kafka.damero.exception.count")
                    .tag("topic", topic)
                    .tag("exception", exceptionType)
                    .register(meterRegistry)
                    .increment();

            Counter.builder("kafka.damero.retry.count")
                    .tag("topic", topic)
                    .tag("attempt", String.valueOf(attempts))
                    .register(meterRegistry)
                    .increment();
        } catch (Exception e) {
            logger.warn("failed to record failure metrics for topic {}: {}", topic, e.getMessage());
        }
    }

    @Override
    public boolean isAvailable() {
        return true;
    }

    @Override
    public void recordBatch(String topic, int batchSize, int processed, int failed,
            long processingTimeMs, boolean fixedWindow) {
        try {
            DistributionSummary.builder("kafka.damero.batch.size")
                    .tag("topic", topic)
                    .tag("mode", fixedWindow ? "fixed_window" : "capacity_first")
                    .register(meterRegistry)
                    .record(batchSize);

            Timer.builder("kafka.damero.batch.processing.time")
                    .tag("topic", topic)
                    .tag("mode", fixedWindow ? "fixed_window" : "capacity_first")
                    .publishPercentiles(0.5, 0.99)
                    .register(meterRegistry)
                    .record(processingTimeMs, java.util.concurrent.TimeUnit.MILLISECONDS);

            Counter.builder("kafka.damero.batch.messages.processed")
                    .tag("topic", topic)
                    .tag("status", "success")
                    .register(meterRegistry)
                    .increment(processed);

            if (failed > 0) {
                Counter.builder("kafka.damero.batch.messages.processed")
                        .tag("topic", topic)
                        .tag("status", "failed")
                        .register(meterRegistry)
                        .increment(failed);
            }

            if (batchSize > 0) {
                double avgTimePerMessage = (double) processingTimeMs / batchSize;
                DistributionSummary.builder("kafka.damero.batch.time.per.message")
                        .tag("topic", topic)
                        .publishPercentiles(0.5, 0.99)
                        .register(meterRegistry)
                        .record(avgTimePerMessage);
            }
        } catch (Exception e) {
            logger.warn("Failed to record batch metrics for topic {}: {}", topic, e.getMessage());
        }
    }

    @Override
    public void recordBatchWindowExpiry(String topic, long messageCount) {
        try {
            Counter.builder("kafka.damero.batch.window.expired")
                    .tag("topic", topic)
                    .register(meterRegistry)
                    .increment();

            DistributionSummary.builder("kafka.damero.batch.window.fill.count")
                    .tag("topic", topic)
                    .register(meterRegistry)
                    .record(messageCount);
        } catch (Exception e) {
            logger.warn("Failed to record batch window expiry metrics for topic {}: {}", topic, e.getMessage());
        }
    }

    @Override
    public void recordBatchCapacityReached(String topic) {
        try {
            Counter.builder("kafka.damero.batch.capacity.reached")
                    .tag("topic", topic)
                    .register(meterRegistry)
                    .increment();
        } catch (Exception e) {
            logger.warn("Failed to record batch capacity metrics for topic {}: {}", topic, e.getMessage());
        }
    }

    @Override
    public void recordBatchBackpressure(String topic) {
        try {
            Counter.builder("kafka.damero.batch.backpressure")
                    .tag("topic", topic)
                    .register(meterRegistry)
                    .increment();
        } catch (Exception e) {
            logger.warn("Failed to record batch backpressure metrics for topic {}: {}", topic, e.getMessage());
        }
    }
}
