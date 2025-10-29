package net.damero.CustomKafkaSetup;

import net.damero.CustomObject.EventMetadata;
import net.damero.CustomObject.EventWrapper;
import net.damero.KafkaServices.KafkaDLQ;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import net.damero.Annotations.CustomKafkaListener;

import java.lang.reflect.Method;
import java.time.LocalDateTime;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Aspect
public class KafkaListenerAspect {

    private final KafkaDLQ kafkaDLQ;
    private final ApplicationContext context;
    private final KafkaTemplate<?, ?> defaultKafkaTemplate;

    private final Map<String, Integer> eventAttempts = new ConcurrentHashMap<>();
    @Autowired
    public KafkaListenerAspect(KafkaDLQ kafkaDLQ,
                               ApplicationContext context,
                               KafkaTemplate<?, ?> defaultKafkaTemplate) {
        this.kafkaDLQ = kafkaDLQ;
        this.context = context;
        this.defaultKafkaTemplate = defaultKafkaTemplate;
    }

    @Around("@annotation(customKafkaListener)")
    public Object kafkaListener(ProceedingJoinPoint pjp, CustomKafkaListener customKafkaListener) throws Throwable {
        System.out.println("🔴🔴🔴 ASPECT TRIGGERED for topic: " + customKafkaListener.topic());
        System.out.println("🔴🔴🔴 Method: " + pjp.getSignature().getName());

        KafkaTemplate<?, ?> kafkaTemplate = resolveKafkaTemplate(customKafkaListener);
        Acknowledgment acknowledgment = extractAcknowledgment(pjp.getArgs());

        try {
            Object result = pjp.proceed();

            // Acknowledge after successful processing
            if (acknowledgment != null) {
                acknowledgment.acknowledge();
                System.out.println("✅ Message processed successfully and acknowledged");
            }

            return result;

        } catch (Throwable e) {
            System.out.println("⚠️ Exception caught in aspect: " + e.getMessage());

            // ✅ ALWAYS acknowledge to prevent Spring from retrying
            if (acknowledgment != null) {
                acknowledgment.acknowledge();
                System.out.println("✅ Acknowledged message after exception");
            }

            // Check if this is a non-retryable exception
            if (isNonRetryableException(e)) {
                System.err.println("❌ Non-retryable exception: " + e.getClass().getSimpleName());
                // For non-retryable, send directly to DLQ
                sendNonRetryableToDLQ(pjp, customKafkaListener, kafkaTemplate, e);
                return null;
            }

            // Handle retryable failure (send to retry topic or DLQ based on attempts)
            handleRetryableFailure(pjp, customKafkaListener, kafkaTemplate, e, acknowledgment);

            // Return null to indicate we've handled the exception
            return null;
        }
    }

    private void sendNonRetryableToDLQ(ProceedingJoinPoint pjp,
                                       CustomKafkaListener customKafkaListener,
                                       KafkaTemplate<?, ?> kafkaTemplate,
                                       Throwable exception) {
        Object rawArg = pjp.getArgs().length > 0 ? pjp.getArgs()[0] : null;
        if (rawArg == null) {
            System.err.println("⚠️ No event found to send to DLQ");
            return;
        }

        Object actualEvent = rawArg instanceof EventWrapper<?> wrapper ? wrapper.getEvent() : rawArg;

        EventMetadata metadata = new EventMetadata(
                LocalDateTime.now(),
                LocalDateTime.now(),
                1,
                customKafkaListener.topic(),
                (long) customKafkaListener.delay(),
                customKafkaListener.delayMethod(),
                customKafkaListener.maxAttempts()
        );

        EventWrapper<Object> wrappedEvent = new EventWrapper<>(actualEvent, LocalDateTime.now(), metadata);

        System.out.println("💀 Sending non-retryable exception to DLQ: " + customKafkaListener.dlqTopic());
        sendToTopic(kafkaTemplate, customKafkaListener.dlqTopic(), wrappedEvent);
    }

    private void handleRetryableFailure(ProceedingJoinPoint pjp,
                                        CustomKafkaListener customKafkaListener,
                                        KafkaTemplate<?, ?> kafkaTemplate,
                                        Throwable exception,
                                        Acknowledgment acknowledgment) {

        Object rawArg = pjp.getArgs().length > 0 ? pjp.getArgs()[0] : null;
        if (rawArg == null) {
            System.err.println("⚠️ No event found in method arguments");
            return;
        }

        Object actualEvent;
        String eventId = null;
        int currentAttempts;
        LocalDateTime firstFailure = LocalDateTime.now();

        if (rawArg instanceof EventWrapper<?> wrapper) {
            actualEvent = wrapper.getEvent();
            EventMetadata metadata = wrapper.getMetadata();
            currentAttempts = metadata.getAttempts();
            firstFailure = metadata.getFirstFailureDateTime();

            // Try to extract event ID
            eventId = extractEventId(actualEvent);
        } else {
            actualEvent = rawArg;
            // ✅ Extract event ID and check if we've seen this before
            eventId = extractEventId(actualEvent);
            currentAttempts = eventAttempts.getOrDefault(eventId, 0);
        }

        int newAttempts = currentAttempts + 1;

        // ✅ Store the new attempt count
        if (eventId != null) {
            eventAttempts.put(eventId, newAttempts);
        }

        LocalDateTime now = LocalDateTime.now();

        EventMetadata updatedMetadata = new EventMetadata(
                firstFailure,
                now,
                newAttempts,
                customKafkaListener.topic(),
                (long) customKafkaListener.delay(),
                customKafkaListener.delayMethod(),
                customKafkaListener.maxAttempts()
        );

        EventWrapper<Object> wrappedEvent = new EventWrapper<>(actualEvent, now, updatedMetadata);

        System.out.println("📊 Event " + eventId + " - Attempt " + newAttempts + "/" + customKafkaListener.maxAttempts());

        if (newAttempts >= customKafkaListener.maxAttempts()) {
            System.out.println("⚠️ Max attempts reached. Sending to DLQ: " + customKafkaListener.dlqTopic());
            // ✅ Clean up tracking
            if (eventId != null) {
                eventAttempts.remove(eventId);
            }
            kafkaDLQ.sendToDLQ(kafkaTemplate, customKafkaListener.dlqTopic(), wrappedEvent, exception, updatedMetadata);

        } else if (customKafkaListener.retryable() && !customKafkaListener.retryableTopic().isEmpty()) {
            System.out.println("🔄 Sending to retry topic: " + customKafkaListener.retryableTopic());
            sendToTopic(kafkaTemplate, customKafkaListener.retryableTopic(), wrappedEvent);

        } else {
            System.out.println("⚠️ No retry configured. Sending to DLQ: " + customKafkaListener.dlqTopic());
            if (eventId != null) {
                eventAttempts.remove(eventId);
            }
            kafkaDLQ.sendToDLQ(kafkaTemplate, customKafkaListener.dlqTopic(), wrappedEvent, exception, updatedMetadata);
        }
    }

    private KafkaTemplate<?, ?> resolveKafkaTemplate(CustomKafkaListener customKafkaListener) {
        Class<?> templateClass = customKafkaListener.kafkaTemplate();

        if (templateClass.equals(void.class)) {
            return defaultKafkaTemplate;
        }

        try {
            return (KafkaTemplate<?, ?>) context.getBean(templateClass);
        } catch (Exception e) {
            System.err.println("⚠️ Failed to resolve custom KafkaTemplate, using default: " + e.getMessage());
            return defaultKafkaTemplate;
        }
    }

    private Acknowledgment extractAcknowledgment(Object[] args) {
        for (Object arg : args) {
            if (arg instanceof Acknowledgment) {
                return (Acknowledgment) arg;
            }
        }
        return null;
    }

    private boolean isNonRetryableException(Throwable e) {
        return e instanceof ClassCastException
                || e instanceof IllegalArgumentException
                || e instanceof IllegalStateException;
    }

//    private void handleRetryableFailure(ProceedingJoinPoint pjp,
//                                        CustomKafkaListener customKafkaListener,
//                                        KafkaTemplate<?, ?> kafkaTemplate,
//                                        Throwable exception,
//                                        Acknowledgment acknowledgment) {
//
//        Object rawArg = pjp.getArgs().length > 0 ? pjp.getArgs()[0] : null;
//        if (rawArg == null) {
//            System.err.println("⚠️ No event found in method arguments");
//            return;
//        }
//
//        Object actualEvent = rawArg instanceof EventWrapper<?> wrapper ? wrapper.getEvent() : rawArg;
//
//        EventMetadata metadata;
//        LocalDateTime firstFailure;
//        int currentAttempts;
//
//        if (rawArg instanceof EventWrapper<?> wrapper) {
//            // already been retried - extract existing metadata
//            metadata = wrapper.getMetadata();
//            firstFailure = metadata.getFirstFailureDateTime() != null
//                    ? metadata.getFirstFailureDateTime()
//                    : LocalDateTime.now();
//            currentAttempts = metadata.getAttempts();
//        } else {
//            // this was the first failure
//            firstFailure = LocalDateTime.now();
//            currentAttempts = 0;
//        }
//
//        int newAttempts = currentAttempts + 1;
//        LocalDateTime now = LocalDateTime.now();
//
//        // UPDATED: Include delay configuration in metadata
//        EventMetadata updatedMetadata = new EventMetadata(
//                firstFailure,
//                now,
//                newAttempts,
//                customKafkaListener.topic(),
//                (long) customKafkaListener.delay(), // Store delay config
//                customKafkaListener.delayMethod()
//                , customKafkaListener.maxAttempts() // Store delay method
//        );
//
//        EventWrapper<Object> wrappedEvent = new EventWrapper<>(actualEvent, now, updatedMetadata);
//
//        if (newAttempts >= customKafkaListener.maxAttempts()) {
//            // max attempts reached - send to DLQ
//            System.out.println("⚠️ Max attempts (" + customKafkaListener.maxAttempts() +
//                    ") reached for event. Sending to DLQ: " + customKafkaListener.dlqTopic());
//            sendToTopic(kafkaTemplate, customKafkaListener.dlqTopic(), wrappedEvent);
//            kafkaDLQ.sendToDLQ(kafkaTemplate, customKafkaListener.dlqTopic(), wrappedEvent, exception, updatedMetadata);
//        } else if (customKafkaListener.retryable() && !customKafkaListener.retryableTopic().isEmpty()) {
//            // Send to retryable topic
//            System.out.println("🔄 Sending to retry topic (" + newAttempts + "/" +
//                    customKafkaListener.maxAttempts() + "): " + customKafkaListener.retryableTopic());
//            sendToTopic(kafkaTemplate, customKafkaListener.retryableTopic(), wrappedEvent);
//        } else {
//            //Retryable is disabled and event is sent to dlq topic
//            System.out.println("⚠️ No retry topic configured. Sending to DLQ: " + customKafkaListener.dlqTopic());
//            sendToTopic(kafkaTemplate, customKafkaListener.dlqTopic(), wrappedEvent);
//            kafkaDLQ.sendToDLQ(kafkaTemplate, customKafkaListener.dlqTopic(), wrappedEvent, exception, updatedMetadata);
//        }
//    }

    private String extractEventId(Object event) {
        try {
            Method getIdMethod = event.getClass().getMethod("getId");
            Object id = getIdMethod.invoke(event);
            return id != null ? id.toString() : null;
        } catch (Exception e) {
            System.err.println("⚠️ Could not extract event ID: " + e.getMessage());
            return null;
        }
    }

    @SuppressWarnings("unchecked")
    private <K, V> void sendToTopic(KafkaTemplate<?, ?> template, String topic, V message) {
        try {
            ((KafkaTemplate<K, V>) template).send(topic, message);
        } catch (Exception e) {
            System.err.println("❌ Failed to send message to topic " + topic + ": " + e.getMessage());
            throw new RuntimeException("Failed to send to Kafka topic: " + topic, e);
        }
    }
}