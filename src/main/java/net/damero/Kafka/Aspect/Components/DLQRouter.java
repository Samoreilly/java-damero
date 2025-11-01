package net.damero.Kafka.Aspect.Components;

import net.damero.Kafka.CustomObject.EventMetadata;
import net.damero.Kafka.CustomObject.EventWrapper;
import net.damero.Kafka.KafkaServices.KafkaDLQ;
import net.damero.Kafka.Annotations.CustomKafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;

/**
 * Component responsible for routing events to Dead Letter Queue.
 */
public class DLQRouter {
    
    private static final Logger logger = LoggerFactory.getLogger(DLQRouter.class);

    public DLQRouter() {
        // No dependencies needed as KafkaDLQ.sendToDLQ is static
    }

    /**
     * Sends an event to DLQ with circuit breaker metadata.
     * 
     * @param kafkaTemplate the Kafka template to use
     * @param originalEvent the original event to send
     * @param customKafkaListener the listener configuration
     */
    public void sendToDLQForCircuitBreakerOpen(KafkaTemplate<?, ?> kafkaTemplate,
                                                Object originalEvent,
                                                CustomKafkaListener customKafkaListener) {
        EventMetadata dlqMetadata = new EventMetadata(
            LocalDateTime.now(),
            LocalDateTime.now(),
            null,
            new RuntimeException("Circuit breaker OPEN - service unavailable"),
            1,
            customKafkaListener.topic(),
            customKafkaListener.dlqTopic(),
            (long) customKafkaListener.delay(),
            customKafkaListener.delayMethod(),
            customKafkaListener.maxAttempts()
        );
        
        EventWrapper<Object> dlqWrapper = new EventWrapper<>(
            originalEvent, 
            LocalDateTime.now(), 
            dlqMetadata
        );
        
        KafkaDLQ.sendToDLQ(
            kafkaTemplate,
            customKafkaListener.dlqTopic(),
            dlqWrapper,
            new RuntimeException("Circuit breaker OPEN"),
            dlqMetadata
        );
        
        logger.info("sent event to dlq due to circuit breaker OPEN for topic: {}", 
            customKafkaListener.topic());
    }

    /**
     * Sends an event to DLQ after max retry attempts reached.
     * 
     * @param kafkaTemplate the Kafka template to use
     * @param originalEvent the original event to send
     * @param exception the exception that occurred
     * @param currentAttempts the current number of attempts
     * @param priorMetadata prior metadata if available
     * @param customKafkaListener the listener configuration
     */
    public void sendToDLQAfterMaxAttempts(KafkaTemplate<?, ?> kafkaTemplate,
                                          Object originalEvent,
                                          Exception exception,
                                          int currentAttempts,
                                          EventMetadata priorMetadata,
                                          CustomKafkaListener customKafkaListener) {
        EventMetadata dlqMetadata = new EventMetadata(
            priorMetadata != null && priorMetadata.getFirstFailureDateTime() != null 
                ? priorMetadata.getFirstFailureDateTime() 
                : LocalDateTime.now(),
            LocalDateTime.now(),
            priorMetadata != null && priorMetadata.getFirstFailureException() != null 
                ? priorMetadata.getFirstFailureException() 
                : exception,
            exception,
            currentAttempts,
            customKafkaListener.topic(),
            customKafkaListener.dlqTopic(),
            (long) customKafkaListener.delay(),
            customKafkaListener.delayMethod(),
            customKafkaListener.maxAttempts()
        );
        
        EventWrapper<Object> dlqWrapper = new EventWrapper<>(
            originalEvent,
            LocalDateTime.now(),
            dlqMetadata
        );
        
        KafkaDLQ.sendToDLQ(
            kafkaTemplate,
            customKafkaListener.dlqTopic(),
            dlqWrapper,
            exception,
            dlqMetadata
        );
        
        logger.info("sent event to dlq after {} attempts for topic: {}", 
            currentAttempts, customKafkaListener.topic());
    }
}

