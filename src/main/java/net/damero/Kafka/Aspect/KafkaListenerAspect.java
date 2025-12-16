package net.damero.Kafka.Aspect;

import net.damero.Kafka.Annotations.DameroKafkaListener;
import net.damero.Kafka.Aspect.Components.Utility.AspectHelperMethods;
import net.damero.Kafka.Aspect.Components.Utility.EventUnwrapper;
import net.damero.Kafka.Aspect.Components.Utility.HeaderUtils;
import net.damero.Kafka.Aspect.Components.Utility.MetricsRecorder;
import net.damero.Kafka.BatchOrchestrator.*;
import net.damero.Kafka.Tracing.TracingSpan;
import net.damero.Kafka.Tracing.TracingContext;
import net.damero.Kafka.Tracing.TracingScope;
import net.damero.Kafka.Aspect.Components.*;
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
    private final BatchUtility batchUtility;

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
                               BatchProcessor batchProcessor,
                               BatchUtility batchUtility) {
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
        this.batchUtility = batchUtility;

        // Register callback for async batch window expiry
        this.batchOrchestrator.setWindowExpiryCallback(this::handleBatchWindowExpiry);
    }

    /**
     * Handle batch window expiry callback from BatchOrchestrator.
     * Called asynchronously when the batch window timer expires.
     */
    private void handleBatchWindowExpiry(String topic, DameroKafkaListener listener) {
        ProceedingJoinPoint pjp = topicJoinPoints.get(topic);
        if (pjp == null) {
            logger.warn("No join point stored for topic: {} - cannot process expired batch", topic);
            return;
        }

        try {
            logger.info("Processing expired batch for topic: {}", topic);
            KafkaTemplate<?, ?> kafkaTemplate = AspectHelperMethods.resolveKafkaTemplate(listener, context, defaultKafkaTemplate);
            batchProcessor.processBatch(pjp, listener, kafkaTemplate);

            // Clean up join point if no more pending messages
            if (!batchProcessor.hasPendingMessages(topic)) {
                topicJoinPoints.remove(topic);
            }
        } catch (Throwable e) {
            logger.error("Error processing expired batch for topic: {}: {}", topic, e.getMessage(), e);
        }
    }


    @Around("@annotation(dameroKafkaListener)")
    public Object kafkaListener(ProceedingJoinPoint pjp, DameroKafkaListener dameroKafkaListener) throws Throwable {


        // BATCH PROCESSING: Collect and defer until capacity reached
        // Window expiry is handled asynchronously via BatchOrchestrator callback
        if (dameroKafkaListener.batchCapacity() > 0) {
            String topic = dameroKafkaListener.topic();

            // Store join point for async window expiry callback (only if not already stored)
            topicJoinPoints.putIfAbsent(topic, pjp);

            BatchStatus status = batchOrchestrator.orchestrate(dameroKafkaListener, topic, pjp.getArgs());

            //checks if batch should still collect or capacity reached
            BatchCheckResult batchCheck = batchUtility.checkBatchStatus(status, context, defaultKafkaTemplate, pjp, dameroKafkaListener, topicJoinPoints, topic);

            if(batchCheck.status() == BatchStatus.PROCESSING) {
                return null;
            }else if(batchCheck.status() == BatchStatus.CAPACITY_REACHED) {
                return batchCheck.object();
            }

        }

        logger.debug("aspect triggered for topic: {}", dameroKafkaListener.topic());
        long processingStartTime = System.currentTimeMillis();

        Acknowledgment acknowledgment = AspectHelperMethods.extractAcknowledgment(pjp.getArgs());

        Object arg0 = pjp.getArgs().length > 0 ? pjp.getArgs()[0] : null;
        
        // Extract ConsumerRecord if present to get headers
        ConsumerRecord<?, ?> consumerRecord = AspectHelperMethods.extractConsumerRecord(arg0);

        EventMetadata existingMetadata = null;
        if (consumerRecord != null) {
            existingMetadata = HeaderUtils.extractMetadataFromHeaders(consumerRecord.headers());
        }
        
        Object event = EventUnwrapper.unwrapEvent(arg0);

        KafkaTemplate<?, ?> kafkaTemplate = AspectHelperMethods.resolveKafkaTemplate(dameroKafkaListener, context, defaultKafkaTemplate);

        if (event == null) {
            logger.warn("no event found in listener arguments for topic: {}", dameroKafkaListener.topic());
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
        if (dameroKafkaListener.openTelemetry()) {
            // Extract parent context from Kafka headers if available
            TracingContext parentContext = consumerRecord != null
                ? tracingService.extractContext(consumerRecord.headers())
                : tracingService.extractContext(null);

            processingSpan = tracingService.startProcessingSpan(
                "damero.kafka.process",
                dameroKafkaListener.topic(),
                eventId
            );

            // Add additional attributes
            processingSpan.setAttribute("damero.retry.enabled", dameroKafkaListener.retryable());
            processingSpan.setAttribute("damero.retry.max_attempts", dameroKafkaListener.maxAttempts());
            processingSpan.setAttribute("damero.dlq.topic", dameroKafkaListener.dlqTopic());
            processingSpan.setAttribute("damero.circuit_breaker.enabled", dameroKafkaListener.enableCircuitBreaker());
            processingSpan.setAttribute("damero.deduplication.enabled", dameroKafkaListener.deDuplication());

            // Make this span the current context
            scope = parentContext.with(processingSpan);
        }

        try {
        // Check circuit breaker if enabled
        Object circuitBreaker = circuitBreakerWrapper.getCircuitBreaker(dameroKafkaListener);
        
        if (circuitBreaker != null && circuitBreakerWrapper.isOpen(circuitBreaker)) {
            logger.warn("circuit breaker open for topic: {} - sending directly to dlq", dameroKafkaListener.topic());
            
            // Add circuit breaker span
            if (dameroKafkaListener.openTelemetry()) {
                TracingSpan cbSpan = tracingService.startCircuitBreakerSpan(dameroKafkaListener.topic(), "OPEN");
                cbSpan.setAttribute("damero.circuit_breaker.action", "send_to_dlq");
                cbSpan.setSuccess();
                cbSpan.end();
            }

            dlqRouter.sendToDLQForCircuitBreakerOpen(kafkaTemplate, originalEvent, dameroKafkaListener);
            
            if (acknowledgment != null) {
                acknowledgment.acknowledge();
            }

            if (dameroKafkaListener.openTelemetry()) {
                processingSpan.setAttribute("damero.circuit_breaker.open", true);
                processingSpan.setAttribute("damero.outcome", "dlq_circuit_breaker");
                processingSpan.setSuccess();
            }

            return null;
        }

        // Check for duplicate BEFORE processing
        if (dameroKafkaListener.deDuplication() && duplicationManager.isDuplicate(eventId)) {
            logger.warn("duplicate message detected for eventId: {} - message was ignored", eventId);

            // Add deduplication span
            if (dameroKafkaListener.openTelemetry()) {
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

            metricsRecorder.recordSuccess(dameroKafkaListener.topic(), processingStartTime);

            if (acknowledgment != null) {
                acknowledgment.acknowledge();
                logger.debug("message processed successfully and acknowledged for topic: {}", dameroKafkaListener.topic());
            }

            // Mark as seen AFTER successful processing to prevent future duplicates
            if (dameroKafkaListener.deDuplication()) {
                duplicationManager.markAsSeen(eventId);

                if (dameroKafkaListener.openTelemetry()) {
                    TracingSpan dedupSpan = tracingService.startDeduplicationSpan(eventId, false);
                    dedupSpan.setAttribute("damero.deduplication.action", "marked_as_seen");
                    dedupSpan.setSuccess();
                    dedupSpan.end();
                }
            }

            // clear attempts on success
            retryOrchestrator.clearAttempts(eventId);
            retrySched.clearFibonacciState(event);

            if (dameroKafkaListener.openTelemetry()) {
                processingSpan.setAttribute("damero.outcome", "success");
                processingSpan.setSuccess();
            }

            return result;

        } catch (Exception e) {

            if (dameroKafkaListener.openTelemetry()) {
                processingSpan.setAttribute("damero.exception.type", e.getClass().getSimpleName());
                processingSpan.setAttribute("damero.exception.message", e.getMessage());
            }

            if(!dameroKafkaListener.retryable()){
                logger.info("retryable is false for topic: {} - sending directly to dlq", dameroKafkaListener.topic());

                metricsRecorder.recordFailure(dameroKafkaListener.topic(), e, 1, processingStartTime);

                dlqRouter.sendToDLQAfterMaxAttempts(
                        kafkaTemplate,
                        originalEvent,
                        e,
                        1,
                        existingMetadata,
                        dameroKafkaListener
                );

                if (dameroKafkaListener.openTelemetry()) {
                    processingSpan.setAttribute("damero.outcome", "dlq_non_retryable");
                    processingSpan.recordException(e);
                }

                return null;  //retryable is false - should not retry

            }

            logger.debug("exception caught during processing for topic: {}: {}",
                dameroKafkaListener.topic(), e.getMessage());
            
            if (acknowledgment != null) {
                acknowledgment.acknowledge();
                logger.debug("acknowledged message after exception for topic: {}", dameroKafkaListener.topic());
            }

            /*
             * Check for conditional DLQ routing with skipRetry=true
             * If match found and skipRetry=true, send to DLQ immediately without retry
             */
            if(dameroKafkaListener.dlqRoutes().length > 0){

                boolean routedToDLQ = dlqExceptionRoutingManager.routeToDLQIfSkipRetry(
                        dameroKafkaListener, kafkaTemplate, originalEvent, e, existingMetadata);

                if(routedToDLQ) {
                    // Message sent to conditional DLQ, stop processing
                    metricsRecorder.recordFailure(dameroKafkaListener.topic(), e, 1, processingStartTime);

                    if (dameroKafkaListener.openTelemetry()) {
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

            if (AspectHelperMethods.isNonRetryableException(e, dameroKafkaListener)) {
                logger.info("exception {} is non-retryable for topic: {} - sending directly to dlq", 
                    e.getClass().getSimpleName(), dameroKafkaListener.topic());
                
                metricsRecorder.recordFailure(dameroKafkaListener.topic(), e, 1, processingStartTime);
                
                dlqRouter.sendToDLQAfterMaxAttempts(
                    kafkaTemplate,
                    originalEvent,
                    e,
                    1,
                    existingMetadata,
                        dameroKafkaListener
                );
                
                if (dameroKafkaListener.openTelemetry()) {
                    processingSpan.setAttribute("damero.outcome", "dlq_non_retryable_exception");
                    processingSpan.setAttribute("damero.non_retryable", true);
                    processingSpan.recordException(e);
                }

                return null;
            }


            //increment by event id to track events across retries
            int currentAttempts = retryOrchestrator.incrementAttempts(eventId);
            metricsRecorder.recordFailure(dameroKafkaListener.topic(), e, currentAttempts, processingStartTime);

            if (retryOrchestrator.hasReachedMaxAttempts(eventId, dameroKafkaListener.maxAttempts())) {
                logger.info("max attempts reached ({}) for event in topic: {}",
                    currentAttempts, dameroKafkaListener.topic());

                // Check for conditional DLQ routing (skipRetry=false routes)
                if(dameroKafkaListener.dlqRoutes().length > 0) {
                    // This method handles routing to conditional DLQ or default DLQ as fallback
                    // It also clears attempts internally
                    dlqExceptionRoutingManager.routeToDLQAfterMaxAttempts(
                            dameroKafkaListener, kafkaTemplate, originalEvent, e, eventId, currentAttempts, existingMetadata);
                } else {
                    // No conditional routing, send to default DLQ
                    dlqRouter.sendToDLQAfterMaxAttempts(
                        kafkaTemplate,
                        originalEvent,
                        e,
                        currentAttempts,
                        existingMetadata,
                            dameroKafkaListener
                    );
                    retryOrchestrator.clearAttempts(eventId);
                }

                if (dameroKafkaListener.openTelemetry()) {
                    processingSpan.setAttribute("damero.outcome", "dlq_max_attempts");
                    processingSpan.setAttribute("damero.retry.exhausted", true);
                    processingSpan.recordException(e);
                }

                return null;
            }

            // Schedule retry with existing metadata from headers
            retryOrchestrator.scheduleRetry(dameroKafkaListener, originalEvent, e, currentAttempts, existingMetadata, kafkaTemplate);

            if (dameroKafkaListener.openTelemetry()) {
                processingSpan.setAttribute("damero.outcome", "retry_scheduled");
                processingSpan.setAttribute("damero.retry.current_attempt", currentAttempts);
                processingSpan.recordException(e);
            }

            return null;
        }
        } finally {
            // Close the span and scope
            if (dameroKafkaListener.openTelemetry() && processingSpan != null) {
                processingSpan.end();
                if (scope != null) {
                    scope.close();
                }
            }
        }
    }


}
