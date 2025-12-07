package net.damero.Kafka.Aspect;

import net.damero.Kafka.Tracing.TracingSpan;
import net.damero.Kafka.Tracing.TracingContext;
import net.damero.Kafka.Tracing.TracingScope;
import net.damero.Kafka.Aspect.Components.*;
import net.damero.Kafka.Annotations.CustomKafkaListener;
import net.damero.Kafka.Aspect.Deduplication.DuplicationManager;
import net.damero.Kafka.Config.PluggableRedisCache;
import net.damero.Kafka.RetryScheduler.RetrySched;
import net.damero.Kafka.Tracing.TracingService;
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
    private final RetrySched retrySched;
    private final DLQExceptionRoutingManager dlqExceptionRoutingManager;
    private final DuplicationManager duplicationManager;
    private final TracingService tracingService;
    private final PluggableRedisCache cache;

    public KafkaListenerAspect(DLQRouter dlqRouter,
                               ApplicationContext context,
                               KafkaTemplate<?, ?> defaultKafkaTemplate,
                               RetryOrchestrator retryOrchestrator,
                               MetricsRecorder metricsRecorder,
                               CircuitBreakerWrapper circuitBreakerWrapper,
                               RetrySched retrySched,
                               DLQExceptionRoutingManager dlqExceptionRoutingManager,
                               DuplicationManager duplicationManager,
                               TracingService tracingService,
                               PluggableRedisCache cache) {
        this.dlqRouter = dlqRouter;
        this.context = context;
        this.defaultKafkaTemplate = defaultKafkaTemplate;
        this.retryOrchestrator = retryOrchestrator;
        this.metricsRecorder = metricsRecorder;
        this.circuitBreakerWrapper = circuitBreakerWrapper;
        this.retrySched = retrySched;
        this.dlqExceptionRoutingManager = dlqExceptionRoutingManager;
        this.duplicationManager = duplicationManager;
        this.tracingService = tracingService;
        this.cache = cache;
    }


    @Around("@annotation(customKafkaListener)")
    public Object kafkaListener(ProceedingJoinPoint pjp, CustomKafkaListener customKafkaListener) throws Throwable {

        // Handle rate limiting per topic
        if (customKafkaListener.messagesPerWindow() > 0 && customKafkaListener.messageWindow() > 0) {
            handleRateLimiting(customKafkaListener, customKafkaListener.openTelemetry());
        }

        logger.debug("aspect triggered for topic: {}", customKafkaListener.topic());
        long processingStartTime = System.currentTimeMillis();

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

        // Start main processing span if tracing is enabled
        TracingSpan processingSpan = null;
        TracingScope scope = null;
        if (customKafkaListener.openTelemetry()) {
            // Extract parent context from Kafka headers if available
            TracingContext parentContext = consumerRecord != null
                ? tracingService.extractContext(consumerRecord.headers())
                : tracingService.extractContext(null);

            processingSpan = tracingService.startProcessingSpan(
                "damero.kafka.process",
                customKafkaListener.topic(),
                eventId
            );

            // Add additional attributes
            processingSpan.setAttribute("damero.retry.enabled", customKafkaListener.retryable());
            processingSpan.setAttribute("damero.retry.max_attempts", customKafkaListener.maxAttempts());
            processingSpan.setAttribute("damero.dlq.topic", customKafkaListener.dlqTopic());
            processingSpan.setAttribute("damero.circuit_breaker.enabled", customKafkaListener.enableCircuitBreaker());
            processingSpan.setAttribute("damero.deduplication.enabled", customKafkaListener.deDuplication());

            // Make this span the current context
            scope = parentContext.with(processingSpan);
        }

        try {
        // Check circuit breaker if enabled
        Object circuitBreaker = circuitBreakerWrapper.getCircuitBreaker(customKafkaListener);
        
        if (circuitBreaker != null && circuitBreakerWrapper.isOpen(circuitBreaker)) {
            logger.warn("circuit breaker open for topic: {} - sending directly to dlq", customKafkaListener.topic());
            
            // Add circuit breaker span
            if (customKafkaListener.openTelemetry()) {
                TracingSpan cbSpan = tracingService.startCircuitBreakerSpan(customKafkaListener.topic(), "OPEN");
                cbSpan.setAttribute("damero.circuit_breaker.action", "send_to_dlq");
                cbSpan.setSuccess();
                cbSpan.end();
            }

            dlqRouter.sendToDLQForCircuitBreakerOpen(kafkaTemplate, originalEvent, customKafkaListener);
            
            if (acknowledgment != null) {
                acknowledgment.acknowledge();
            }

            if (customKafkaListener.openTelemetry()) {
                processingSpan.setAttribute("damero.circuit_breaker.open", true);
                processingSpan.setAttribute("damero.outcome", "dlq_circuit_breaker");
                processingSpan.setSuccess();
            }

            return null;
        }

        // Check for duplicate BEFORE processing
        if (customKafkaListener.deDuplication() && duplicationManager.isDuplicate(eventId)) {
            logger.warn("duplicate message detected for eventId: {} - message was ignored", eventId);

            // Add deduplication span
            if (customKafkaListener.openTelemetry()) {
                TracingSpan dedupSpan = tracingService.startDeduplicationSpan(eventId, true);
                dedupSpan.setAttribute("damero.deduplication.action", "ignored");
                dedupSpan.setSuccess();
                dedupSpan.end();

                processingSpan.setAttribute("damero.outcome", "duplicate_ignored");
                processingSpan.setSuccess();
            }

            if (acknowledgment != null) {
                acknowledgment.acknowledge(); // stops infinite delivery from producer
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

            metricsRecorder.recordSuccess(customKafkaListener.topic(), processingStartTime);

            if (acknowledgment != null) {
                acknowledgment.acknowledge();
                logger.debug("message processed successfully and acknowledged for topic: {}", customKafkaListener.topic());
            }

            // Mark as seen AFTER successful processing to prevent future duplicates
            if (customKafkaListener.deDuplication()) {
                duplicationManager.markAsSeen(eventId);

                if (customKafkaListener.openTelemetry()) {
                    TracingSpan dedupSpan = tracingService.startDeduplicationSpan(eventId, false);
                    dedupSpan.setAttribute("damero.deduplication.action", "marked_as_seen");
                    dedupSpan.setSuccess();
                    dedupSpan.end();
                }
            }

            // clear attempts on success
            retryOrchestrator.clearAttempts(eventId);
            retrySched.clearFibonacciState(event);

            if (customKafkaListener.openTelemetry()) {
                processingSpan.setAttribute("damero.outcome", "success");
                processingSpan.setSuccess();
            }

            return result;

        } catch (Exception e) {

            if (customKafkaListener.openTelemetry()) {
                processingSpan.setAttribute("damero.exception.type", e.getClass().getSimpleName());
                processingSpan.setAttribute("damero.exception.message", e.getMessage());
            }

            if(!customKafkaListener.retryable()){
                logger.info("retryable is false for topic: {} - sending directly to dlq", customKafkaListener.topic());

                metricsRecorder.recordFailure(customKafkaListener.topic(), e, 1, processingStartTime);

                dlqRouter.sendToDLQAfterMaxAttempts(
                        kafkaTemplate,
                        originalEvent,
                        e,
                        1,
                        existingMetadata,
                        customKafkaListener
                );

                if (customKafkaListener.openTelemetry()) {
                    processingSpan.setAttribute("damero.outcome", "dlq_non_retryable");
                    processingSpan.recordException(e);
                }

                return null;  //retryable is false - should not retry

            }

            logger.debug("exception caught during processing for topic: {}: {}",
                customKafkaListener.topic(), e.getMessage());
            
            if (acknowledgment != null) {
                acknowledgment.acknowledge();
                logger.debug("acknowledged message after exception for topic: {}", customKafkaListener.topic());
            }

            /*
             * Check for conditional DLQ routing with skipRetry=true
             * If match found and skipRetry=true, send to DLQ immediately without retry
             */
            if(customKafkaListener.dlqRoutes().length > 0){

                boolean routedToDLQ = dlqExceptionRoutingManager.routeToDLQIfSkipRetry(
                    customKafkaListener, kafkaTemplate, originalEvent, e, existingMetadata);

                if(routedToDLQ) {
                    // Message sent to conditional DLQ, stop processing
                    metricsRecorder.recordFailure(customKafkaListener.topic(), e, 1, processingStartTime);

                    if (customKafkaListener.openTelemetry()) {
                        processingSpan.setAttribute("damero.outcome", "dlq_conditional_skip_retry");
                        processingSpan.recordException(e);
                    }

                    return null;
                }
                // else: skipRetry=false or no matching route, continue with retry logic
            }

            /*
             * USERS WILL PROVIDE IT IN THIS FORMAT
             *  nonRetryableExceptions = {IllegalArgumentException.class, ValidationException.class}
             *  This does not retry events with these exceptions
             */

            if (isNonRetryableException(e, customKafkaListener)) {
                logger.info("exception {} is non-retryable for topic: {} - sending directly to dlq", 
                    e.getClass().getSimpleName(), customKafkaListener.topic());
                
                metricsRecorder.recordFailure(customKafkaListener.topic(), e, 1, processingStartTime);
                
                dlqRouter.sendToDLQAfterMaxAttempts(
                    kafkaTemplate,
                    originalEvent,
                    e,
                    1,
                    existingMetadata,
                    customKafkaListener
                );
                
                if (customKafkaListener.openTelemetry()) {
                    processingSpan.setAttribute("damero.outcome", "dlq_non_retryable_exception");
                    processingSpan.setAttribute("damero.non_retryable", true);
                    processingSpan.recordException(e);
                }

                return null;
            }


            //increment by event id to track events across retries
            int currentAttempts = retryOrchestrator.incrementAttempts(eventId);
            metricsRecorder.recordFailure(customKafkaListener.topic(), e, currentAttempts, processingStartTime);

            if (retryOrchestrator.hasReachedMaxAttempts(eventId, customKafkaListener.maxAttempts())) {
                logger.info("max attempts reached ({}) for event in topic: {}",
                    currentAttempts, customKafkaListener.topic());

                // Check for conditional DLQ routing (skipRetry=false routes)
                if(customKafkaListener.dlqRoutes().length > 0) {
                    // This method handles routing to conditional DLQ or default DLQ as fallback
                    // It also clears attempts internally
                    dlqExceptionRoutingManager.routeToDLQAfterMaxAttempts(
                        customKafkaListener, kafkaTemplate, originalEvent, e, eventId, currentAttempts, existingMetadata);
                } else {
                    // No conditional routing, send to default DLQ
                    dlqRouter.sendToDLQAfterMaxAttempts(
                        kafkaTemplate,
                        originalEvent,
                        e,
                        currentAttempts,
                        existingMetadata,
                        customKafkaListener
                    );
                    retryOrchestrator.clearAttempts(eventId);
                }

                if (customKafkaListener.openTelemetry()) {
                    processingSpan.setAttribute("damero.outcome", "dlq_max_attempts");
                    processingSpan.setAttribute("damero.retry.exhausted", true);
                    processingSpan.recordException(e);
                }

                return null;
            }

            // Schedule retry with existing metadata from headers
            retryOrchestrator.scheduleRetry(customKafkaListener, originalEvent, e, currentAttempts, existingMetadata, kafkaTemplate);

            if (customKafkaListener.openTelemetry()) {
                processingSpan.setAttribute("damero.outcome", "retry_scheduled");
                processingSpan.setAttribute("damero.retry.current_attempt", currentAttempts);
                processingSpan.recordException(e);
            }

            return null;
        }
        } finally {
            // Close the span and scope
            if (customKafkaListener.openTelemetry() && processingSpan != null) {
                processingSpan.end();
                if (scope != null) {
                    scope.close();
                }
            }
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

    /**
     * Handles rate limiting per topic using a fixed window algorithm.
     * Uses PluggableRedisCache for distributed rate limiting state.
     * Each topic has its own independent rate limiting state.
     *
     * @param customKafkaListener the listener configuration
     * @param enableTracing whether to create trace spans for rate limiting
     * @throws InterruptedException if thread is interrupted during sleep
     */
    private void handleRateLimiting(CustomKafkaListener customKafkaListener, boolean enableTracing) throws InterruptedException {

        String topic = customKafkaListener.topic();
        int messagesPerWindow = customKafkaListener.messagesPerWindow();
        long messageWindowMs = customKafkaListener.messageWindow();

        long now = System.currentTimeMillis();
        long windowStart = cache.getRateLimitWindowStart(topic);

        // if no window exists, initialize it
        if (windowStart == 0) {
            cache.resetRateLimitWindow(topic, now);
            cache.incrementRateLimitCounter(topic);
            return;
        }

        long elapsed = now - windowStart;

        // check if window has expired
        if (elapsed >= messageWindowMs) {
            cache.resetRateLimitWindow(topic, now);
            return;
        }

        // increment counter
        long currentCount = cache.incrementRateLimitCounter(topic);

        // if more messages than allowed
        if (currentCount > messagesPerWindow) {
            long sleepTime = messageWindowMs - elapsed;

            if (sleepTime > 0) {
                logger.debug("Topic: {}, Rate limit reached ({}/{} messages), sleeping for {} ms",
                    topic, currentCount, messagesPerWindow, sleepTime);

                TracingSpan rateLimitSpan = null;
                if (enableTracing) {
                    rateLimitSpan = tracingService.startRateLimitSpan(topic, (int) currentCount, messagesPerWindow, sleepTime);
                }

                try {
                    Thread.sleep(sleepTime);
                    // reset window after sleep
                    cache.resetRateLimitWindow(topic, System.currentTimeMillis());

                    if (enableTracing && rateLimitSpan != null) {
                        rateLimitSpan.setSuccess();
                    }
                } catch (InterruptedException e) {
                    if (enableTracing && rateLimitSpan != null) {
                        rateLimitSpan.recordException(e);
                    }
                    throw e;
                } finally {
                    if (enableTracing && rateLimitSpan != null) {
                        rateLimitSpan.end();
                    }
                }
            } else {
                // window expired during check, reset it
                cache.resetRateLimitWindow(topic, System.currentTimeMillis());
            }
        } else if (enableTracing) {
            TracingSpan rateLimitSpan = tracingService.startRateLimitSpan(topic, (int) currentCount, messagesPerWindow, 0);
            rateLimitSpan.setSuccess();
            rateLimitSpan.end();
        }
    }
}
