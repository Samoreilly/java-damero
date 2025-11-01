package net.damero.Kafka.Aspect.Components;

import io.micrometer.core.instrument.Counter;
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
}

