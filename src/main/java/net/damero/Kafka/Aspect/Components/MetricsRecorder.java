package net.damero.Kafka.Aspect.Components;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.lang.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Component responsible for recording metrics using Micrometer.
 */
public class MetricsRecorder {
    
    private static final Logger logger = LoggerFactory.getLogger(MetricsRecorder.class);
    
    @Nullable
    private final MeterRegistry meterRegistry;

    public MetricsRecorder(@Nullable MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
    }

    /**
     * Records successful message processing metrics.
     * 
     * @param topic the Kafka topic
     * @param startTime the start time in milliseconds
     */
    public void recordSuccess(String topic, long startTime) {
        if (meterRegistry == null) {
            return;
        }
        
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

    /**
     * Records failed message processing metrics.
     * 
     * @param topic the Kafka topic
     * @param exception the exception that occurred
     * @param attempts the number of attempts
     * @param startTime the start time in milliseconds
     */
    public void recordFailure(String topic, Exception exception, int attempts, long startTime) {
        if (meterRegistry == null) {
            return;
        }
        
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

    /**
     * Checks if metrics recording is available.
     * 
     * @return true if meter registry is available
     */
    public boolean isAvailable() {
        return meterRegistry != null;
    }

    /**
     * Records batch processing metrics.
     *
     * @param topic the Kafka topic
     * @param batchSize the number of messages in the batch
     * @param processed the number of successfully processed messages
     * @param failed the number of failed messages
     * @param processingTimeMs the total batch processing time in milliseconds
     * @param fixedWindow whether fixed window mode was used
     */
    public void recordBatch(String topic, int batchSize, int processed, int failed,
                           long processingTimeMs, boolean fixedWindow) {
        if (meterRegistry == null) {
            return;
        }

        try {
            // Batch size histogram
            DistributionSummary.builder("kafka.damero.batch.size")
                .tag("topic", topic)
                .tag("mode", fixedWindow ? "fixed_window" : "capacity_first")
                .register(meterRegistry)
                .record(batchSize);

            // Batch processing time
            Timer.builder("kafka.damero.batch.processing.time")
                .tag("topic", topic)
                .tag("mode", fixedWindow ? "fixed_window" : "capacity_first")
                .register(meterRegistry)
                .record(processingTimeMs, java.util.concurrent.TimeUnit.MILLISECONDS);

            // Success/failure counts
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

            // Per-message processing time (average)
            if (batchSize > 0) {
                double avgTimePerMessage = (double) processingTimeMs / batchSize;
                DistributionSummary.builder("kafka.damero.batch.time.per.message")
                    .tag("topic", topic)
                    .register(meterRegistry)
                    .record(avgTimePerMessage);
            }
        } catch (Exception e) {
            logger.warn("Failed to record batch metrics for topic {}: {}", topic, e.getMessage());
        }
    }

    /**
     * Records batch window expiry event.
     *
     * @param topic the Kafka topic
     * @param messageCount the number of messages in the batch when window expired
     */
    public void recordBatchWindowExpiry(String topic, long messageCount) {
        if (meterRegistry == null) {
            return;
        }

        try {
            Counter.builder("kafka.damero.batch.window.expired")
                .tag("topic", topic)
                .register(meterRegistry)
                .increment();

            // Record how full the batch was when window expired
            DistributionSummary.builder("kafka.damero.batch.window.fill.count")
                .tag("topic", topic)
                .register(meterRegistry)
                .record(messageCount);
        } catch (Exception e) {
            logger.warn("Failed to record batch window expiry metrics for topic {}: {}", topic, e.getMessage());
        }
    }

    /**
     * Records batch capacity reached event.
     *
     * @param topic the Kafka topic
     */
    public void recordBatchCapacityReached(String topic) {
        if (meterRegistry == null) {
            return;
        }

        try {
            Counter.builder("kafka.damero.batch.capacity.reached")
                .tag("topic", topic)
                .register(meterRegistry)
                .increment();
        } catch (Exception e) {
            logger.warn("Failed to record batch capacity metrics for topic {}: {}", topic, e.getMessage());
        }
    }

    /**
     * Records batch backpressure event (when messages are rejected due to full batch).
     *
     * @param topic the Kafka topic
     */
    public void recordBatchBackpressure(String topic) {
        if (meterRegistry == null) {
            return;
        }

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

