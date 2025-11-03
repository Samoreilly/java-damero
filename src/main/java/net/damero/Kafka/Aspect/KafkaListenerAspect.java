package net.damero.Kafka.Aspect;

import net.damero.Kafka.Aspect.Components.CircuitBreakerWrapper;
import net.damero.Kafka.Aspect.Components.DLQRouter;
import net.damero.Kafka.Aspect.Components.EventUnwrapper;
import net.damero.Kafka.Aspect.Components.MetricsRecorder;
import net.damero.Kafka.Aspect.Components.RetryOrchestrator;
import net.damero.Kafka.CustomObject.EventWrapper;
import net.damero.Kafka.Annotations.CustomKafkaListener;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.springframework.context.ApplicationContext;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import net.damero.Kafka.CustomObject.EventMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Aspect that intercepts @CustomKafkaListener annotated methods to provide
 * retry logic, DLQ routing, circuit breaker, and metrics functionality.
 */
@Aspect
@Component
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

    @Around("@annotation(customKafkaListener)")
    public Object kafkaListener(ProceedingJoinPoint pjp, CustomKafkaListener customKafkaListener) throws Throwable {
        logger.debug("aspect triggered for topic: {}", customKafkaListener.topic());
        
        long startTime = System.currentTimeMillis();

        Acknowledgment acknowledgment = extractAcknowledgment(pjp.getArgs());

        Object arg0 = pjp.getArgs().length > 0 ? pjp.getArgs()[0] : null;
        Object event = EventUnwrapper.unwrapEvent(arg0);

        KafkaTemplate<?, ?> kafkaTemplate = resolveKafkaTemplate(customKafkaListener);

        if (event == null) {
            logger.warn("no event found in listener arguments for topic: {}", customKafkaListener.topic());
            if (acknowledgment != null) {
                acknowledgment.acknowledge();
            }
            return null;
        }

        EventWrapper<?> wrappedEvent = event instanceof EventWrapper<?> wrapper 
            ? wrapper 
            : wrapObject(event, customKafkaListener);

        // Check circuit breaker if enabled
        Object circuitBreaker = circuitBreakerWrapper.getCircuitBreaker(customKafkaListener);
        
        if (circuitBreaker != null && circuitBreakerWrapper.isOpen(circuitBreaker)) {
            logger.warn("circuit breaker open for topic: {} - sending directly to dlq", customKafkaListener.topic());
            
            Object originalEvent = EventUnwrapper.extractOriginalEvent(event);
            dlqRouter.sendToDLQForCircuitBreakerOpen(kafkaTemplate, originalEvent, customKafkaListener);
            
            if (acknowledgment != null) {
                acknowledgment.acknowledge();
            }
            return null;
        }

        try {
            // Execute with circuit breaker tracking if enabled
            Object result;
            if (circuitBreaker != null) {
                result = circuitBreakerWrapper.execute(circuitBreaker, pjp);
            } else {
                result = pjp.proceed();
            }

            // Success path
            metricsRecorder.recordSuccess(customKafkaListener.topic(), startTime);

            if (acknowledgment != null) {
                acknowledgment.acknowledge();
                logger.debug("message processed successfully and acknowledged for topic: {}", customKafkaListener.topic());
            }

            return result;

        } catch (Exception e) {
            
            logger.debug("exception caught during processing for topic: {}: {}", 
                customKafkaListener.topic(), e.getMessage());
            
            if (acknowledgment != null) {
                acknowledgment.acknowledge();
                logger.debug("acknowledged message after exception for topic: {}", customKafkaListener.topic());
            }

            Object originalEvent = EventUnwrapper.extractOriginalEvent(event);
            String eventId = EventUnwrapper.extractEventId(originalEvent);

            // Check if exception is non-retryable, if true send to dlq
            /*
             * USERS WILL PROVIDE IT IN THIS FORMAT
             *  nonRetryableExceptions = {IllegalArgumentException.class, ValidationException.class}
             */
            if (isNonRetryableException(e, customKafkaListener)) {
                logger.info("exception {} is non-retryable for topic: {} - sending directly to dlq", 
                    e.getClass().getSimpleName(), customKafkaListener.topic());
                
                metricsRecorder.recordFailure(customKafkaListener.topic(), e, 1, startTime);
                
                EventMetadata prior = 
                    (event instanceof EventWrapper<?> wePrior) ? wePrior.getMetadata() : wrappedEvent.getMetadata();
                
                dlqRouter.sendToDLQAfterMaxAttempts(
                    kafkaTemplate,
                    originalEvent,
                    e,
                    1,
                    prior,
                    customKafkaListener
                );
                
                return null;
            }
            int currentAttempts = retryOrchestrator.incrementAttempts(eventId);
            metricsRecorder.recordFailure(customKafkaListener.topic(), e, currentAttempts, startTime);

            if (retryOrchestrator.hasReachedMaxAttempts(eventId, customKafkaListener.maxAttempts())) {
                logger.info("max attempts reached ({}) for event in topic: {} - sending to dlq: {}", 
                    currentAttempts, customKafkaListener.topic(), customKafkaListener.dlqTopic());
                
                EventMetadata prior = 
                    (event instanceof EventWrapper<?> wePrior) ? wePrior.getMetadata() : wrappedEvent.getMetadata();
                
                dlqRouter.sendToDLQAfterMaxAttempts(
                    kafkaTemplate,
                    originalEvent,
                    e,
                    currentAttempts,
                    prior,
                    customKafkaListener
                );
                
                retryOrchestrator.clearAttempts(eventId);
                return null;
            }

            // Schedule retry
            retryOrchestrator.scheduleRetry(customKafkaListener, originalEvent, e, currentAttempts, kafkaTemplate);

            return null;
        }
    }

    /**
     * Wraps a new message (not an EventWrapper) with EventWrapper and EventMetadata.
     * 
     * @param event the event to wrap
     * @param customKafkaListener the listener configuration
     * @return the wrapped event
     */
    private EventWrapper<?> wrapObject(Object event, CustomKafkaListener customKafkaListener) {
        net.damero.Kafka.CustomObject.EventMetadata metadata = new net.damero.Kafka.CustomObject.EventMetadata(
            java.time.LocalDateTime.now(),
            java.time.LocalDateTime.now(),
            null,
            null,
            0,
            customKafkaListener.topic(),
            customKafkaListener.dlqTopic(),
            (long) customKafkaListener.delay(),
            customKafkaListener.delayMethod(),
            customKafkaListener.maxAttempts()
        );
        return new EventWrapper<>(event, java.time.LocalDateTime.now(), metadata);
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
