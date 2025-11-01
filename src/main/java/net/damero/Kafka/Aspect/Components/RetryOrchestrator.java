package net.damero.Kafka.Aspect.Components;

import net.damero.Kafka.CustomObject.EventMetadata;
import net.damero.Kafka.CustomObject.EventWrapper;
import net.damero.Kafka.Annotations.CustomKafkaListener;
import net.damero.Kafka.RetryScheduler.RetrySched;
import org.springframework.kafka.core.KafkaTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Component responsible for orchestrating retry logic and attempt tracking.
 */
public class RetryOrchestrator {
    
    private static final Logger logger = LoggerFactory.getLogger(RetryOrchestrator.class);
    
    private final RetrySched retrySched;
    private final Map<String, Integer> eventAttempts = new ConcurrentHashMap<>();

    public RetryOrchestrator(RetrySched retrySched) {
        this.retrySched = retrySched;
    }

    /**
     * Increments the attempt count for an event and returns the new count.
     * 
     * @param eventId the event ID
     * @return the new attempt count
     */
    public int incrementAttempts(String eventId) {
        int currentAttempts = eventAttempts.getOrDefault(eventId, 0) + 1;
        eventAttempts.put(eventId, currentAttempts);
        return currentAttempts;
    }

    /**
     * Gets the current attempt count for an event.
     * 
     * @param eventId the event ID
     * @return the current attempt count
     */
    public int getAttemptCount(String eventId) {
        return eventAttempts.getOrDefault(eventId, 0);
    }

    /**
     * Removes attempt tracking for an event.
     * 
     * @param eventId the event ID
     */
    public void clearAttempts(String eventId) {
        if (eventId != null) {
            eventAttempts.remove(eventId);
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
     * Schedules a retry for the given event.
     * 
     * @param customKafkaListener the listener configuration
     * @param originalEvent the original event
     * @param exception the exception that occurred
     * @param currentAttempts the current attempt count
     * @param kafkaTemplate the Kafka template to use
     */
    public void scheduleRetry(CustomKafkaListener customKafkaListener,
                              Object originalEvent,
                              Exception exception,
                              int currentAttempts,
                              KafkaTemplate<?, ?> kafkaTemplate) {
        EventWrapper<Object> retryWrapper = new EventWrapper<>(
            originalEvent,
            LocalDateTime.now(),
            new EventMetadata(
                LocalDateTime.now(),
                LocalDateTime.now(),
                null,
                exception,
                currentAttempts,
                customKafkaListener.topic(),
                customKafkaListener.dlqTopic(),
                (long) customKafkaListener.delay(),
                customKafkaListener.delayMethod(),
                customKafkaListener.maxAttempts()
            )
        );
        
        retrySched.scheduleRetry(customKafkaListener, retryWrapper, kafkaTemplate);
        
        logger.debug("scheduled retry attempt {} for event in topic: {}", 
            currentAttempts, customKafkaListener.topic());
    }
}

