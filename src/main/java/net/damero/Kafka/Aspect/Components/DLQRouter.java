package net.damero.Kafka.Aspect.Components;

import net.damero.Kafka.Annotations.DameroKafkaListener;
import net.damero.Kafka.Tracing.TracingSpan;
import net.damero.Kafka.CustomObject.EventMetadata;
import net.damero.Kafka.CustomObject.EventWrapper;
import net.damero.Kafka.KafkaServices.KafkaDLQ;
import net.damero.Kafka.Tracing.TracingService;
import org.springframework.kafka.core.KafkaTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;

/**
 * Component responsible for routing events to Dead Letter Queue.
 * Supports both default DLQ routing and conditional DLQ routing based on
 * exception types.
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
     * @param kafkaTemplate       the Kafka template to use
     * @param originalEvent       the original event to send
     * @param dameroKafkaListener the listener configuration
     * @param eventId             the canonical event ID
     */
    public void sendToDLQForCircuitBreakerOpen(KafkaTemplate<?, ?> kafkaTemplate,
            Object originalEvent,
            DameroKafkaListener dameroKafkaListener,
            String eventId) {
        TracingSpan dlqSpan = null;
        if (dameroKafkaListener.openTelemetry()) {
            dlqSpan = tracingService.startDLQSpan(
                    dameroKafkaListener.topic(),
                    dameroKafkaListener.dlqTopic(),
                    eventId,
                    1,
                    "circuit_breaker_open");
        }

        try {
            EventMetadata dlqMetadata = new EventMetadata(
                    LocalDateTime.now(),
                    LocalDateTime.now(),
                    null,
                    new RuntimeException("Circuit breaker OPEN - service unavailable"),
                    1,
                    dameroKafkaListener.topic(),
                    dameroKafkaListener.dlqTopic(),
                    (long) dameroKafkaListener.delay(),
                    dameroKafkaListener.delayMethod(),
                    dameroKafkaListener.maxAttempts());

            EventWrapper<Object> dlqWrapper = new EventWrapper<>(
                    originalEvent,
                    LocalDateTime.now(),
                    dlqMetadata);

            KafkaDLQ.sendToDLQ(
                    kafkaTemplate,
                    dameroKafkaListener.dlqTopic(),
                    dlqWrapper);

            logger.info("sent event to dlq due to circuit breaker OPEN for topic: {}",
                    dameroKafkaListener.topic());

            if (dameroKafkaListener.openTelemetry() && dlqSpan != null) {
                dlqSpan.setSuccess();
            }
        } catch (Exception e) {
            if (dameroKafkaListener.openTelemetry() && dlqSpan != null) {
                dlqSpan.recordException(e);
            }
            throw e;
        } finally {
            if (dameroKafkaListener.openTelemetry() && dlqSpan != null) {
                dlqSpan.end();
            }
        }
    }

    /**
     * Sends an event to the default DLQ topic after max retry attempts reached.
     *
     * @param kafkaTemplate       the Kafka template to use
     * @param originalEvent       the original event to send
     * @param exception           the exception that occurred
     * @param currentAttempts     the current number of attempts
     * @param priorMetadata       prior metadata if available
     * @param eventId             the canonical event ID
     * @param dameroKafkaListener the listener configuration
     */
    public void sendToDLQAfterMaxAttempts(KafkaTemplate<?, ?> kafkaTemplate,
            Object originalEvent,
            Exception exception,
            int currentAttempts,
            EventMetadata priorMetadata,
            String eventId,
            DameroKafkaListener dameroKafkaListener) {
        sendToDLQAfterMaxAttempts(
                kafkaTemplate,
                originalEvent,
                exception,
                currentAttempts,
                priorMetadata,
                eventId,
                dameroKafkaListener.dlqTopic(), // Use default DLQ topic
                dameroKafkaListener);
    }

    /**
     * Sends an event to a custom DLQ topic after max retry attempts reached.
     * This overload supports conditional DLQ routing where different exceptions
     * can be routed to different DLQ topics.
     *
     * @param kafkaTemplate       the Kafka template to use
     * @param originalEvent       the original event to send
     * @param exception           the exception that occurred
     * @param currentAttempts     the current number of attempts
     * @param priorMetadata       prior metadata if available
     * @param eventId             the canonical event ID
     * @param customDlqTopic      the custom DLQ topic to send to (overrides
     *                            default)
     * @param dameroKafkaListener the listener configuration
     */
    public void sendToDLQAfterMaxAttempts(KafkaTemplate<?, ?> kafkaTemplate,
            Object originalEvent,
            Exception exception,
            int currentAttempts,
            EventMetadata priorMetadata,
            String eventId,
            String customDlqTopic,
            DameroKafkaListener dameroKafkaListener) {
        TracingSpan dlqSpan = null;
        if (dameroKafkaListener.openTelemetry()) {
            dlqSpan = tracingService.startDLQSpan(
                    dameroKafkaListener.topic(),
                    customDlqTopic,
                    eventId,
                    currentAttempts,
                    "max_attempts_reached");
            dlqSpan.setAttribute("damero.exception.type", exception.getClass().getSimpleName());
            dlqSpan.setAttribute("damero.dlq.skip_retry", currentAttempts == 1);
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
                    dameroKafkaListener.topic(),
                    customDlqTopic, // Use the custom DLQ topic (not the default)
                    (long) dameroKafkaListener.delay(),
                    dameroKafkaListener.delayMethod(),
                    dameroKafkaListener.maxAttempts());

            EventWrapper<Object> dlqWrapper = new EventWrapper<>(
                    originalEvent,
                    LocalDateTime.now(),
                    dlqMetadata);

            KafkaDLQ.sendToDLQ(
                    kafkaTemplate,
                    customDlqTopic, // Send to the custom DLQ topic
                    dlqWrapper);

            logger.info("sent event to custom dlq '{}' after {} attempts for topic: {}",
                    customDlqTopic, currentAttempts, dameroKafkaListener.topic());

            if (dameroKafkaListener.openTelemetry() && dlqSpan != null) {
                dlqSpan.setSuccess();
            }
        } catch (Exception e) {
            if (dameroKafkaListener.openTelemetry() && dlqSpan != null) {
                dlqSpan.recordException(e);
            }
            throw e;
        } finally {
            if (dameroKafkaListener.openTelemetry() && dlqSpan != null) {
                dlqSpan.end();
            }
        }
    }
}
