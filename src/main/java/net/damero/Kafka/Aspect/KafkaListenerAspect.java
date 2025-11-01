package net.damero.Kafka.Aspect;

import net.damero.Kafka.CustomObject.EventMetadata;
import net.damero.Kafka.CustomObject.EventWrapper;
import net.damero.Kafka.KafkaServices.KafkaDLQ;
import net.damero.Kafka.Resilience.CircuitBreakerService;
import net.damero.Kafka.RetryScheduler.RetrySched;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Component;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import net.damero.Kafka.Annotations.CustomKafkaListener;
import java.lang.reflect.Method;
import java.time.LocalDateTime;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

@Aspect
@Component
public class KafkaListenerAspect {

    private final KafkaDLQ kafkaDLQ;
    private final ApplicationContext context;
    private final KafkaTemplate<?, ?> defaultKafkaTemplate;
    private final RetrySched retrySched;

    private final Map<String, Integer> eventAttempts = new ConcurrentHashMap<>();

    @Nullable //Can be null if null if user doesnt have Micrometer
    private final MeterRegistry meterRegistry;
    
    @Nullable
    private final CircuitBreakerService circuitBreakerService;

    // @Autowired is optional here - Spring autowires single constructors automatically
    public KafkaListenerAspect(KafkaDLQ kafkaDLQ,
                               ApplicationContext context,
                               KafkaTemplate<?, ?> defaultKafkaTemplate,
                               RetrySched retrySched,
                               @Nullable MeterRegistry meterRegistry,
                               @Nullable CircuitBreakerService circuitBreakerService) {
        this.kafkaDLQ = kafkaDLQ;
        this.context = context;
        this.defaultKafkaTemplate = defaultKafkaTemplate;
        this.retrySched = retrySched;
        this.meterRegistry = meterRegistry;  // Can be null if users don't have Micrometer
        this.circuitBreakerService = circuitBreakerService;  // Can be null if Resilience4j not available
    }

    @Around("@annotation(customKafkaListener)")
    public Object kafkaListener(ProceedingJoinPoint pjp, CustomKafkaListener customKafkaListener) throws Throwable{
        System.out.println("🔴ASPECT TRIGGERED for topic: " + customKafkaListener.topic());
        
        //Record start time
        long startTime = System.currentTimeMillis();

        Acknowledgment acknowledgment = extractAcknowledgment(pjp.getArgs());

        Object arg0 = pjp.getArgs().length > 0 ? pjp.getArgs()[0] : null;

        // Normalize to the actual payload value (unwrap ConsumerRecord/EventWrapper)
        Object event;
        if (arg0 instanceof ConsumerRecord<?, ?> record) {
            event = record.value();
        } else if (arg0 instanceof EventWrapper<?> ew) {
            Object inner = ew.getEvent();
            if (inner instanceof ConsumerRecord<?, ?> innerRec) {
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

        // Check circuit breaker BEFORE processing (if enabled)
        Object circuitBreaker = null;
        if (customKafkaListener.enableCircuitBreaker() && circuitBreakerService != null && circuitBreakerService.isAvailable()) {
            circuitBreaker = circuitBreakerService.getCircuitBreaker(
                customKafkaListener.topic(),
                customKafkaListener.circuitBreakerFailureThreshold(),
                customKafkaListener.circuitBreakerWindowDuration(),
                customKafkaListener.circuitBreakerWaitDuration()
            );
            
            // If circuit is OPEN, skip processing and go straight to DLQ
            if (circuitBreaker != null && isCircuitBreakerOpen(circuitBreaker)) {
                System.err.println("Circuit breaker OPEN for topic: " + customKafkaListener.topic() + " - sending directly to DLQ");
                
                Object originalEvent = (event instanceof EventWrapper<?> we) ? we.getEvent() : event;
                if (originalEvent instanceof ConsumerRecord<?, ?> rec) {
                    originalEvent = rec.value();
                }
                
                // Create DLQ metadata to send to DLQ
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
                //wrap the original event with the DLQ metadata in EventWrapper
                EventWrapper<Object> dlqWrapper = new EventWrapper<>(originalEvent, LocalDateTime.now(), dlqMetadata);
                kafkaDLQ.sendToDLQ(kafkaTemplate, customKafkaListener.dlqTopic(), dlqWrapper, 
                                  new RuntimeException("Circuit breaker OPEN"), dlqMetadata);
                
                if (acknowledgment != null) {
                    acknowledgment.acknowledge();//acknowledge the message to avoid retries
                }
                return null;  // return to skip retry logic
            }
        }

        try {
            // Execute with circuit breaker tracking if enabled
            Object result;
            if (circuitBreaker != null) {
                result = executeWithCircuitBreaker(circuitBreaker, pjp);
            } else {
                // normal execution without circuit breaker
                result = pjp.proceed();
            }

            recordSuccessMetrics(customKafkaListener.topic(), startTime);

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

            //Record failure metrics for the eventId
            recordFailureMetrics(customKafkaListener.topic(), e, eventAttempts.getOrDefault(eventId, 0), startTime);

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
            System.err.println("Failed to send message to topic " + topic + ": " + e.getMessage());
            throw new RuntimeException("Failed to send to Kafka topic: " + topic, e);
        }
    }

    private String extractEventId(Object event) {
        try {
            Method getIdMethod = event.getClass().getMethod("getId");
            Object id = getIdMethod.invoke(event);
            return id != null ? id.toString() : null;
        } catch (Exception e) {
            System.err.println("Could not extract event ID: " + e.getMessage());
            return null;
        }
    }

    private void recordSuccessMetrics(String topic, long startTime) {
        if (meterRegistry != null) {
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
        }
    }


    private void recordFailureMetrics(String topic, Exception e, int attempts, long startTime) {
        if (meterRegistry != null) {
            String exceptionType = e.getClass().getSimpleName();
            
            Timer.builder("kafka.damero.processing.time")
                .tag("topic", topic)
                .tag("status", "failure")
                .register(meterRegistry)
                .record(System.currentTimeMillis() - startTime, TimeUnit.MILLISECONDS);
            
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
        }
    }

    /**
     * Check if circuit breaker is OPEN using reflection (to avoid compile-time dependency).
     */
    private boolean isCircuitBreakerOpen(Object circuitBreaker) {
        try {
            Method getStateMethod = circuitBreaker.getClass().getMethod("getState");
            Object state = getStateMethod.invoke(circuitBreaker);
            
            // Check if state is OPEN
            Method nameMethod = state.getClass().getMethod("name");
            String stateName = (String) nameMethod.invoke(state);
            
            return "OPEN".equals(stateName);
        } catch (Exception e) {
            // If reflection fails, assume circuit breaker is not open (fallback)
            return false;
        }
    }

    /**
     * Execute with circuit breaker tracking using reflection (to avoid compile-time dependency).
     */
    private Object executeWithCircuitBreaker(Object circuitBreaker, ProceedingJoinPoint pjp) throws Throwable {
        try {
            // Use circuit breaker's executeSupplier method via reflection
            Method executeSupplierMethod = circuitBreaker.getClass().getMethod("executeSupplier", 
                Supplier.class);
            
            Supplier<Object> supplier = () -> {
                try {
                    return pjp.proceed();
                } catch (Throwable throwable) {
                    if (throwable instanceof RuntimeException) {
                        throw (RuntimeException) throwable;
                    }
                    throw new RuntimeException(throwable);
                }
            };
            
            return executeSupplierMethod.invoke(circuitBreaker, supplier);
        } catch (Exception e) {
            // If reflection fails, fall back to normal execution
            return pjp.proceed();
        }
    }

}
