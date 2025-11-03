package net.damero.Kafka.Aspect;

import net.damero.Kafka.Aspect.Components.CircuitBreakerWrapper;
import net.damero.Kafka.Aspect.Components.DLQRouter;
import net.damero.Kafka.Aspect.Components.EventUnwrapper;
import net.damero.Kafka.Aspect.Components.HeaderUtils;
import net.damero.Kafka.Aspect.Components.MetricsRecorder;
import net.damero.Kafka.Aspect.Components.RetryOrchestrator;
import net.damero.Kafka.CustomObject.EventWrapper;
import net.damero.Kafka.Annotations.CustomKafkaListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.springframework.context.ApplicationContext;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import net.damero.Kafka.CustomObject.EventMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Aspect that intercepts @CustomKafkaListener annotated methods to provide
 * retry logic, DLQ routing, circuit breaker, and metrics functionality.
 */
@Aspect
public class KafkaListenerAspect {

    private static final Logger logger = LoggerFactory.getLogger(KafkaListenerAspect.class);

    private final DLQRouter dlqRouter;
    private final ApplicationContext context;
    private final KafkaTemplate<?, ?> defaultKafkaTemplate;
    private final RetryOrchestrator retryOrchestrator;
    private final MetricsRecorder metricsRecorder;
    private final CircuitBreakerWrapper circuitBreakerWrapper;

    public KafkaListenerAspect(DLQRouter dlqRouter,
                               ApplicationContext context,
                               KafkaTemplate<?, ?> defaultKafkaTemplate,
                               RetryOrchestrator retryOrchestrator,
                               MetricsRecorder metricsRecorder,
                               CircuitBreakerWrapper circuitBreakerWrapper) {
        this.dlqRouter = dlqRouter;
        this.context = context;
        this.defaultKafkaTemplate = defaultKafkaTemplate;
        this.retryOrchestrator = retryOrchestrator;
        this.metricsRecorder = metricsRecorder;
        this.circuitBreakerWrapper = circuitBreakerWrapper;
    }

    /**
        PLAN TO FIX SENDING DIFFERENT OBJECTS
        ON RECEIVING AN EVENT, ATTACH HEADERS IF THE EVENT HAS NOT BEEN RETRIED OTHERWISE
        ATTACH HEADERS TO THE OBJECT AND SEND IT BACK
        PROS OF THIS METHOD: USER RECEIVED SEND OBJECT THEY SENT,
        JUST WITH SOME EXTRAS HEADERS FOR METADATA TO TRACK LAST FAILURE, FIRST FAILURE, ATTEMPTS etc
     */

    @Around("@annotation(customKafkaListener)")
    public Object kafkaListener(ProceedingJoinPoint pjp, CustomKafkaListener customKafkaListener) throws Throwable {
        logger.debug("aspect triggered for topic: {}", customKafkaListener.topic());
        
        long startTime = System.currentTimeMillis();

        Acknowledgment acknowledgment = extractAcknowledgment(pjp.getArgs());

        Object arg0 = pjp.getArgs().length > 0 ? pjp.getArgs()[0] : null;
        
        // Extract ConsumerRecord if present to get headers
        ConsumerRecord<?, ?> consumerRecord = extractConsumerRecord(arg0);
        EventMetadata existingMetadata = null;
        if (consumerRecord != null) {
            existingMetadata = HeaderUtils.extractMetadataFromHeaders(consumerRecord.headers());
        }
        
        Object event = EventUnwrapper.unwrapEvent(arg0);

        KafkaTemplate<?, ?> kafkaTemplate = resolveKafkaTemplate(customKafkaListener);

        if (event == null) {
            logger.warn("no event found in listener arguments for topic: {}", customKafkaListener.topic());
            if (acknowledgment != null) {
                acknowledgment.acknowledge();
            }
            return null;
        }
        
        Object originalEvent = EventUnwrapper.extractOriginalEvent(event);
        String eventId = EventUnwrapper.extractEventId(originalEvent);

        // Check circuit breaker if enabled
        Object circuitBreaker = circuitBreakerWrapper.getCircuitBreaker(customKafkaListener);
        
        if (circuitBreaker != null && circuitBreakerWrapper.isOpen(circuitBreaker)) {
            logger.warn("circuit breaker open for topic: {} - sending directly to dlq", customKafkaListener.topic());
            
            dlqRouter.sendToDLQForCircuitBreakerOpen(kafkaTemplate, originalEvent, customKafkaListener);
            
            if (acknowledgment != null) {
                acknowledgment.acknowledge();
            }
            return null;
        }

        try {
            // execute circuit breaker tracking if enabled
            Object result;
            if (circuitBreaker != null) {
                result = circuitBreakerWrapper.execute(circuitBreaker, pjp);
            } else {
                result = pjp.proceed();
            }

            metricsRecorder.recordSuccess(customKafkaListener.topic(), startTime);

            if (acknowledgment != null) {
                acknowledgment.acknowledge();
                logger.debug("message processed successfully and acknowledged for topic: {}", customKafkaListener.topic());
            }

            // Clear attempts on success
            retryOrchestrator.clearAttempts(eventId);

            return result;

        } catch (Exception e) {
            
            logger.debug("exception caught during processing for topic: {}: {}", 
                customKafkaListener.topic(), e.getMessage());
            
            if (acknowledgment != null) {
                acknowledgment.acknowledge();
                logger.debug("acknowledged message after exception for topic: {}", customKafkaListener.topic());
            }

            // Check if exception is non-retryable, if true send to dlq
            /*
             * USERS WILL PROVIDE IT IN THIS FORMAT
             *  nonRetryableExceptions = {IllegalArgumentException.class, ValidationException.class}
             */
            if (isNonRetryableException(e, customKafkaListener)) {
                logger.info("exception {} is non-retryable for topic: {} - sending directly to dlq", 
                    e.getClass().getSimpleName(), customKafkaListener.topic());
                
                metricsRecorder.recordFailure(customKafkaListener.topic(), e, 1, startTime);
                
                dlqRouter.sendToDLQAfterMaxAttempts(
                    kafkaTemplate,
                    originalEvent,
                    e,
                    1,
                    existingMetadata,
                    customKafkaListener
                );
                
                return null;
            }
            //increment by event id to track events across retries
            int currentAttempts = retryOrchestrator.incrementAttempts(eventId);
            metricsRecorder.recordFailure(customKafkaListener.topic(), e, currentAttempts, startTime);

            if (retryOrchestrator.hasReachedMaxAttempts(eventId, customKafkaListener.maxAttempts())) {
                logger.info("max attempts reached ({}) for event in topic: {} - sending to dlq: {}", 
                    currentAttempts, customKafkaListener.topic(), customKafkaListener.dlqTopic());
                
                dlqRouter.sendToDLQAfterMaxAttempts(
                    kafkaTemplate,
                    originalEvent,
                    e,
                    currentAttempts,
                    existingMetadata,
                    customKafkaListener
                );
                
                retryOrchestrator.clearAttempts(eventId);
                return null;
            }

            // Schedule retry with existing metadata from headers
            retryOrchestrator.scheduleRetry(customKafkaListener, originalEvent, e, currentAttempts, existingMetadata, kafkaTemplate);

            return null;
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
            logger.warn("failed to resolve custom kafka template {}, using default: {}", 
                templateClass.getName(), e.getMessage());
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

    /**
     * Extracts ConsumerRecord from method arguments if present.
     * 
     * @param arg the first argument from the join point
     * @return ConsumerRecord if present, null otherwise
     */
    @SuppressWarnings("unchecked")
    private ConsumerRecord<?, ?> extractConsumerRecord(Object arg) {
        if (arg instanceof ConsumerRecord<?, ?>) {
            return (ConsumerRecord<?, ?>) arg;
        }
        return null;
    }

    /**
     * Checks if an exception is non-retryable based on the configured nonRetryableExceptions.
     * If nonRetryableExceptions is empty, all exceptions are retryable.
     * 
     * @param exception the exception to check
     * @param customKafkaListener the listener configuration
     * @return true if the exception is non-retryable (should go to DLQ), false if it should be retried
     */
    
    private boolean isNonRetryableException(Exception exception, CustomKafkaListener customKafkaListener) {
        Class<? extends Throwable>[] nonRetryableExceptions = customKafkaListener.nonRetryableExceptions();
        
        // If no non-retryable exceptions specified, all exceptions are retryable
        if (nonRetryableExceptions == null || nonRetryableExceptions.length == 0) {
            return false;
        }
        
        Class<?> exceptionClass = exception.getClass();
        for (Class<? extends Throwable> nonRetryableException : nonRetryableExceptions) {
            if (nonRetryableException != null && nonRetryableException.isAssignableFrom(exceptionClass)) {
                return true;
            }
        }
        
        return false;
    }
}
