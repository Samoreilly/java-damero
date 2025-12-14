package net.damero.Kafka.Aspect;

import net.damero.Kafka.BatchOrchestrator.BatchOrchestrator;
import net.damero.Kafka.BatchOrchestrator.BatchProcessor;
import net.damero.Kafka.BatchOrchestrator.BatchStatus;
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
import java.util.concurrent.ConcurrentHashMap;


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
    private final BatchOrchestrator batchOrchestrator;
    private final BatchProcessor batchProcessor;

    // Store ProceedingJoinPoint per topic for async batch processing
    private final ConcurrentHashMap<String, ProceedingJoinPoint> topicJoinPoints = new ConcurrentHashMap<>();

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
                               PluggableRedisCache cache,
                               BatchOrchestrator batchOrchestrator,
                               BatchProcessor batchProcessor) {
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
        this.batchOrchestrator = batchOrchestrator;
        this.batchProcessor = batchProcessor;

        // Register callback for async batch window expiry
        this.batchOrchestrator.setWindowExpiryCallback(this::handleBatchWindowExpiry);
    }

    /**
     * Handle batch window expiry callback from BatchOrchestrator.
     * Called asynchronously when the batch window timer expires.
     */
    private void handleBatchWindowExpiry(String topic, CustomKafkaListener listener) {
        ProceedingJoinPoint pjp = topicJoinPoints.get(topic);
        if (pjp == null) {
            logger.warn("No join point stored for topic: {} - cannot process expired batch", topic);
            return;
        }

        try {
            logger.info("Processing expired batch for topic: {}", topic);
            KafkaTemplate<?, ?> kafkaTemplate = resolveKafkaTemplate(listener);
            batchProcessor.processBatch(pjp, listener, kafkaTemplate);

            // Clean up join point if no more pending messages
            if (!batchProcessor.hasPendingMessages(topic)) {
                topicJoinPoints.remove(topic);
            }
        } catch (Throwable e) {
            logger.error("Error processing expired batch for topic: {}: {}", topic, e.getMessage(), e);
        }
    }


    @Around("@annotation(customKafkaListener)")
    public Object kafkaListener(ProceedingJoinPoint pjp, CustomKafkaListener customKafkaListener) throws Throwable {


        // BATCH PROCESSING: Collect and defer until capacity reached
        // Window expiry is handled asynchronously via BatchOrchestrator callback
        if (customKafkaListener.batchCapacity() > 0) {
            String topic = customKafkaListener.topic();

            // Store join point for async window expiry callback (only if not already stored)
            topicJoinPoints.putIfAbsent(topic, pjp);

            BatchStatus status = batchOrchestrator.orchestrate(customKafkaListener, topic, pjp.getArgs());

            if (status == BatchStatus.PROCESSING) {
                // Still collecting - DO NOT acknowledge yet
                // Messages will be redelivered if server crashes before batch completes
                // Deduplication will handle any reprocessing after recovery
                logger.debug("Batch collecting for topic: {} - message queued (unacknowledged)", topic);
                return null;
            }

            if (status == BatchStatus.CAPACITY_REACHED) {
                // Capacity reached - process the batch immediately
                logger.info("Batch capacity reached for topic: {} - processing batch", topic);
                KafkaTemplate<?, ?> kafkaTemplate = resolveKafkaTemplate(customKafkaListener);
                Object result = batchProcessor.processBatch(pjp, customKafkaListener, kafkaTemplate);

                // Clean up join point if no more pending messages
                if (!batchProcessor.hasPendingMessages(topic)) {
                    topicJoinPoints.remove(topic);
                }
                return result;
            }
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

}
