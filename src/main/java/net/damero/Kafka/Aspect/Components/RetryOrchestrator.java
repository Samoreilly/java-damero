package net.damero.Kafka.Aspect.Components;

import net.damero.Kafka.Config.PluggableRedisCache;
import net.damero.Kafka.CustomObject.EventMetadata;
import net.damero.Kafka.Annotations.CustomKafkaListener;
import net.damero.Kafka.RetryScheduler.RetrySched;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.springframework.kafka.core.KafkaTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;

/**
 * Component responsible for orchestrating retry logic and attempt tracking.
 */
@Component
public class RetryOrchestrator {
    
    private static final Logger logger = LoggerFactory.getLogger(RetryOrchestrator.class);
    
    private final RetrySched retrySched;
    private final PluggableRedisCache cache;

    public RetryOrchestrator(RetrySched retrySched, PluggableRedisCache cache) {
        this.retrySched = retrySched;
        this.cache = cache;
    }

    /**
     * Increments the attempt count for an event and returns the new count.
     * 
     * @param eventId the event ID
     * @return the new attempt count
     */
    public int incrementAttempts(String eventId) {
        int currentAttempts = cache.getOrDefault(eventId, 0) + 1;
        cache.put(eventId, currentAttempts);
        return currentAttempts;
    }

    /**
     * Gets the current attempt count for an event.
     * 
     * @param eventId the event ID
     * @return the current attempt count
     */
    public int getAttemptCount(String eventId) {
        return cache.getOrDefault(eventId, 1);
    }

    /**
     * Removes attempt tracking for an event.
     * 
     * @param eventId the event ID
     */
    public void clearAttempts(String eventId) {
        if (eventId != null) {
            cache.remove(eventId);
        }
    }

    /**
     * Checks if max attempts have been reached.
     * 
     * @param eventId the event ID
     * @param maxAttempts the maximum number of attempts
     * @return true if max attempts reached
     */
    public boolean hasReachedMaxAttempts(String eventId, int maxAttempts) {
        return getAttemptCount(eventId) >= maxAttempts;
    }

    /**
     * Schedules a retry for the given event with headers-based metadata.
     * 
     * @param customKafkaListener the listener configuration
     * @param originalEvent the original event
     * @param exception the exception that occurred
     * @param currentAttempts the current attempt count
     * @param existingMetadata existing metadata from headers (if any)
     * @param kafkaTemplate the Kafka template to use
     */
    public void scheduleRetry(CustomKafkaListener customKafkaListener,
                              Object originalEvent,
                              Exception exception,
                              int currentAttempts,
                              EventMetadata existingMetadata,
                              KafkaTemplate<?, ?> kafkaTemplate) {
        // Build metadata for the retry
        EventMetadata configMetadata = new EventMetadata(
            existingMetadata != null && existingMetadata.getFirstFailureDateTime() != null 
                ? existingMetadata.getFirstFailureDateTime() 
                : LocalDateTime.now(),
            LocalDateTime.now(),
            existingMetadata != null && existingMetadata.getFirstFailureException() != null 
                ? existingMetadata.getFirstFailureException() 
                : exception,
            exception,
            currentAttempts,
            customKafkaListener.topic(),
            customKafkaListener.dlqTopic(),
            (long) customKafkaListener.delay(),
            customKafkaListener.delayMethod(),
            customKafkaListener.maxAttempts()
        );
        
        // Build headers from metadata
        RecordHeaders headers = HeaderUtils.buildHeadersFromMetadata(
            existingMetadata,
            configMetadata,
            currentAttempts,
            exception
        );
        
        // Schedule retry with original event and headers
        retrySched.scheduleRetry(customKafkaListener, originalEvent, headers, kafkaTemplate);
        
        logger.debug("scheduled retry attempt {} for event in topic: {}", 
            currentAttempts, customKafkaListener.topic());
    }
}

