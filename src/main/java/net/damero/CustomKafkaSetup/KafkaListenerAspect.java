package net.damero.CustomKafkaSetup;

import net.damero.CustomObject.EventMetadata;
import net.damero.CustomObject.EventWrapper;
import net.damero.KafkaServices.KafkaDLQ;
import net.damero.RetryScheduler.RetrySched;
import org.apache.kafka.clients.consumer.ConsumerRecord;
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
@Component
public class KafkaListenerAspect {

    private final KafkaDLQ kafkaDLQ;
    private final ApplicationContext context;
    private final KafkaTemplate<?, ?> defaultKafkaTemplate;
    private final RetrySched retrySched;

    private final Map<String, Integer> eventAttempts = new ConcurrentHashMap<>();

    @Autowired
    public KafkaListenerAspect(KafkaDLQ kafkaDLQ,
                               ApplicationContext context,
                               KafkaTemplate<?, ?> defaultKafkaTemplate,
                               RetrySched retrySched) {
        this.kafkaDLQ = kafkaDLQ;
        this.context = context;
        this.defaultKafkaTemplate = defaultKafkaTemplate;
        this.retrySched = retrySched;
    }

    @Around("@annotation(customKafkaListener)")
    public Object kafkaListener(ProceedingJoinPoint pjp, CustomKafkaListener customKafkaListener) throws Throwable{
        System.out.println("🔴 ASPECT TRIGGERED for topic: " + customKafkaListener.topic());

        Acknowledgment acknowledgment = extractAcknowledgment(pjp.getArgs());

        Object arg0 = pjp.getArgs().length > 0 ? pjp.getArgs()[0] : null;

        // Normalize to the actual payload value (unwrap ConsumerRecord/EventWrapper)
        Object event;
        if (arg0 instanceof ConsumerRecord<?, ?> record) {
            event = record.value();
        } else if (arg0 instanceof EventWrapper<?> ew) {
            Object inner = ew.getEvent();
            if (inner instanceof org.apache.kafka.clients.consumer.ConsumerRecord<?, ?> innerRec) {
                event = innerRec.value();
            } else {
                event = inner;
            }
        } else {
            event = arg0;
        }

        KafkaTemplate<?, ?> kafkaTemplate = resolveKafkaTemplate(customKafkaListener);

        if (event == null) {
            System.err.println("No event found to send to DLQ");
            return null;
        }

        EventWrapper<?> wrappedEvent;

        if (event instanceof EventWrapper<?> wrapper) {
            wrappedEvent = wrapper;
        } else {
            wrappedEvent = wrapObject(event, customKafkaListener);
        }

        try {

            Object result = pjp.proceed();

            if (acknowledgment != null) {
                acknowledgment.acknowledge();
                System.out.println("Message processed successfully and acknowledged");
            }

            return result;

        } catch (Exception e) {

            if (acknowledgment != null) {
                acknowledgment.acknowledge();
                System.out.println("Acknowledged message after exception");
            }

            Object originalEvent = (event instanceof EventWrapper<?> we) ? we.getEvent() : event;
            if (originalEvent instanceof ConsumerRecord<?, ?> rec) {
                originalEvent = rec.value();
            }
            String eventId = extractEventId(originalEvent);
            int currentAttempts = eventAttempts.getOrDefault(eventId, 0) + 1;
            eventAttempts.put(eventId, currentAttempts);

            if (currentAttempts >= customKafkaListener.maxAttempts()) {
                System.err.println("Max attempts reached. Sending to DLQ: " + customKafkaListener.dlqTopic());
                EventMetadata prior = (event instanceof EventWrapper<?> wePrior) ? wePrior.getMetadata() : wrappedEvent.getMetadata();
                EventMetadata dlqMetadata = new EventMetadata(
                        prior != null && prior.getFirstFailureDateTime() != null ? prior.getFirstFailureDateTime() : LocalDateTime.now(),
                        LocalDateTime.now(),
                        prior != null && prior.getFirstFailureException() != null ? prior.getFirstFailureException() : e,
                        e,
                        currentAttempts,
                        customKafkaListener.topic(),
                        customKafkaListener.dlqTopic(),
                        (long) customKafkaListener.delay(),
                        customKafkaListener.delayMethod(),
                        customKafkaListener.maxAttempts()
                );
                EventWrapper<Object> dlqWrapper = new EventWrapper<>(originalEvent, LocalDateTime.now(), dlqMetadata);
                kafkaDLQ.sendToDLQ(kafkaTemplate, customKafkaListener.dlqTopic(), dlqWrapper, e, dlqMetadata);
                if (eventId != null) {
                    eventAttempts.remove(eventId);
                }
                return null;
            }

            // schedule retry by sending the original event back to the original topic
            EventWrapper<Object> retryWrapper = new EventWrapper<>(originalEvent, LocalDateTime.now(), new EventMetadata(
                    LocalDateTime.now(),
                    LocalDateTime.now(),
                    null,
                    e,
                    currentAttempts,
                    customKafkaListener.topic(),
                    customKafkaListener.dlqTopic(),
                    (long) customKafkaListener.delay(),
                    customKafkaListener.delayMethod(),
                    customKafkaListener.maxAttempts()
            ));
            retrySched.scheduleRetry(customKafkaListener, retryWrapper, kafkaTemplate);

            return null;
        }
    }

    /*
        This method takes in a new message(not a EventWrapper) from the queue and wraps in EventWrapper and EventMetaData to be sent back into the queue with metadata
    */

    public EventWrapper<?> wrapObject(Object event, CustomKafkaListener customKafkaListener){

        EventMetadata metadata = new EventMetadata(
                LocalDateTime.now(),
                LocalDateTime.now(),
                null,
                null,
                0,
                customKafkaListener.topic(),
                customKafkaListener.dlqTopic(),
                (long) customKafkaListener.delay(),
                customKafkaListener.delayMethod(),
                customKafkaListener.maxAttempts()
        );
        return new EventWrapper<>(event, LocalDateTime.now(), metadata);
    }

    /*
        UPDATE METADATA WITH NEW DATA | ATTEMPTS + 1 | RECENT FAILURE etc
    */

    public EventMetadata updateMetadata(EventMetadata oldMetadata, Exception failure){

        return oldMetadata.toBuilder()
                .attempts(oldMetadata.getAttempts() + 1)
                .firstFailureException(oldMetadata.getFirstFailureException() != null ? oldMetadata.getFirstFailureException() : failure)
                .lastFailureException(failure)
                .firstFailureDateTime(oldMetadata.getFirstFailureDateTime() != null ? oldMetadata.getFirstFailureDateTime() : LocalDateTime.now())
                .lastFailureDateTime(LocalDateTime.now())
                .build();

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

    @SuppressWarnings("unchecked")
    public static <K, V> void sendToTopic(KafkaTemplate<?, ?> template, String topic, V message) {
        try {
            ((KafkaTemplate<K, V>) template).send(topic, message);
        } catch (Exception e) {
            System.err.println("❌ Failed to send message to topic " + topic + ": " + e.getMessage());
            throw new RuntimeException("Failed to send to Kafka topic: " + topic, e);
        }
    }

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
}
