package net.damero.Kafka.Aspect.Components;

import net.damero.Kafka.Aspect.Components.Utility.EventUnwrapper;
import net.damero.Kafka.Tracing.TracingSpan;
import net.damero.Kafka.CustomObject.EventMetadata;
import net.damero.Kafka.CustomObject.EventWrapper;
import net.damero.Kafka.KafkaServices.KafkaDLQ;
import net.damero.Kafka.Annotations.CustomKafkaListener;
import net.damero.Kafka.Tracing.TracingService;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;

/**
 * Component responsible for routing events to Dead Letter Queue.
 * Supports both default DLQ routing and conditional DLQ routing based on exception types.
 */
public class DLQRouter {
    
    private static final Logger logger = LoggerFactory.getLogger(DLQRouter.class);

    private final TracingService tracingService;

    public DLQRouter(TracingService tracingService) {
        this.tracingService = tracingService;
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
        TracingSpan dlqSpan = null;
        if (customKafkaListener.openTelemetry()) {
            String eventId = EventUnwrapper.extractEventId(originalEvent);
            dlqSpan = tracingService.startDLQSpan(
                customKafkaListener.topic(),
                customKafkaListener.dlqTopic(),
                eventId,
                1,
                "circuit_breaker_open"
            );
        }

        try {
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
                dlqWrapper
            );

            logger.info("sent event to dlq due to circuit breaker OPEN for topic: {}",
                customKafkaListener.topic());

            if (customKafkaListener.openTelemetry() && dlqSpan != null) {
                dlqSpan.setSuccess();
            }
        } catch (Exception e) {
            if (customKafkaListener.openTelemetry() && dlqSpan != null) {
                dlqSpan.recordException(e);
            }
            throw e;
        } finally {
            if (customKafkaListener.openTelemetry() && dlqSpan != null) {
                dlqSpan.end();
            }
        }
    }
    /*
    OVERLOADED METHOD FOR OPEN TELEMETRY TRACING
     */
    public void sendToDLQForCircuitBreakerOpen(KafkaTemplate<?, ?> kafkaTemplate, ProducerRecord<String, Object> record, CustomKafkaListener customKafkaListener) {
        Object originalEvent = record.value();

        TracingSpan dlqSpan = null;
        if (customKafkaListener.openTelemetry()) {
            // Extract context for potential parent span linking (not used directly but good practice)
            tracingService.extractContext(record.headers());
            String eventId = EventUnwrapper.extractEventId(originalEvent);
            dlqSpan = tracingService.startDLQSpan(
                customKafkaListener.topic(),
                customKafkaListener.dlqTopic(),
                eventId,
                1,
                "circuit_breaker_open"
            );
        }

        try {
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
                    dlqWrapper
            );

            logger.info("sent event to dlq due to circuit breaker OPEN for topic: {}",
                    customKafkaListener.topic());

            if (customKafkaListener.openTelemetry() && dlqSpan != null) {
                dlqSpan.setSuccess();
            }
        } catch (Exception e) {
            if (customKafkaListener.openTelemetry() && dlqSpan != null) {
                dlqSpan.recordException(e);
            }
            throw e;
        } finally {
            if (customKafkaListener.openTelemetry() && dlqSpan != null) {
                dlqSpan.end();
            }
        }
    }

    /**
     * Sends an event to the default DLQ topic after max retry attempts reached.
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
        sendToDLQAfterMaxAttempts(
            kafkaTemplate,
            originalEvent,
            exception,
            currentAttempts,
            priorMetadata,
            customKafkaListener.dlqTopic(),  // Use default DLQ topic
            customKafkaListener
        );
    }

    /**
     * Sends an event to a custom DLQ topic after max retry attempts reached.
     * This overload supports conditional DLQ routing where different exceptions
     * can be routed to different DLQ topics.
     *
     * @param kafkaTemplate the Kafka template to use
     * @param originalEvent the original event to send
     * @param exception the exception that occurred
     * @param currentAttempts the current number of attempts
     * @param priorMetadata prior metadata if available
     * @param customDlqTopic the custom DLQ topic to send to (overrides default)
     * @param customKafkaListener the listener configuration
     */
    public void sendToDLQAfterMaxAttempts(KafkaTemplate<?, ?> kafkaTemplate,
                                          Object originalEvent,
                                          Exception exception,
                                          int currentAttempts,
                                          EventMetadata priorMetadata,
                                          String customDlqTopic,
                                          CustomKafkaListener customKafkaListener) {
        TracingSpan dlqSpan = null;
        if (customKafkaListener.openTelemetry()) {
            String eventId = EventUnwrapper.extractEventId(originalEvent);
            dlqSpan = tracingService.startDLQSpan(
                customKafkaListener.topic(),
                customDlqTopic,
                eventId,
                currentAttempts,
                "max_attempts_reached"
            );
            dlqSpan.setAttribute("damero.exception.type", exception.getClass().getSimpleName());
        }

        try {
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
                customDlqTopic,  // Use the custom DLQ topic (not the default)
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
                customDlqTopic,  // Send to the custom DLQ topic
                dlqWrapper
            );

            logger.info("sent event to custom dlq '{}' after {} attempts for topic: {}",
                customDlqTopic, currentAttempts, customKafkaListener.topic());

            if (customKafkaListener.openTelemetry() && dlqSpan != null) {
                dlqSpan.setSuccess();
            }
        } catch (Exception e) {
            if (customKafkaListener.openTelemetry() && dlqSpan != null) {
                dlqSpan.recordException(e);
            }
            throw e;
        } finally {
            if (customKafkaListener.openTelemetry() && dlqSpan != null) {
                dlqSpan.end();
            }
        }
    }

}

