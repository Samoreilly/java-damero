package net.damero.Kafka.BatchOrchestrator;

import net.damero.Kafka.Annotations.DameroKafkaListener;
import net.damero.Kafka.Aspect.Components.*;
import net.damero.Kafka.Aspect.Components.Utility.EventUnwrapper;
import net.damero.Kafka.Aspect.Components.Utility.HeaderUtils;
import net.damero.Kafka.Aspect.Components.Utility.MetricsRecorder;
import net.damero.Kafka.Aspect.Deduplication.DuplicationManager;
import net.damero.Kafka.CustomObject.EventMetadata;
import net.damero.Kafka.RetryScheduler.RetrySched;
import net.damero.Kafka.Tracing.TracingScope;
import net.damero.Kafka.Tracing.TracingService;
import net.damero.Kafka.Tracing.TracingSpan;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.aspectj.lang.ProceedingJoinPoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Handles batch processing of Kafka messages.
 * Extracted from KafkaListenerAspect to improve modularity.
 *
 * Responsibilities:
 * - Processing batches of messages
 * - Managing batch acknowledgments (deferred until after processing)
 * - Tracing for batch operations
 */
@Component
public class BatchProcessor {

    private static final Logger logger = LoggerFactory.getLogger(BatchProcessor.class);

    private final BatchOrchestrator batchOrchestrator;
    private final DuplicationManager duplicationManager;
    private final CircuitBreakerWrapper circuitBreakerWrapper;
    private final DLQRouter dlqRouter;
    private final MetricsRecorder metricsRecorder;
    private final RetryOrchestrator retryOrchestrator;
    private final RetrySched retrySched;
    private final TracingService tracingService;

    public BatchProcessor(BatchOrchestrator batchOrchestrator,
                          DuplicationManager duplicationManager,
                          CircuitBreakerWrapper circuitBreakerWrapper,
                          DLQRouter dlqRouter,
                          MetricsRecorder metricsRecorder,
                          RetryOrchestrator retryOrchestrator,
                          RetrySched retrySched,
                          TracingService tracingService) {
        this.batchOrchestrator = batchOrchestrator;
        this.duplicationManager = duplicationManager;
        this.circuitBreakerWrapper = circuitBreakerWrapper;
        this.dlqRouter = dlqRouter;
        this.metricsRecorder = metricsRecorder;
        this.retryOrchestrator = retryOrchestrator;
        this.retrySched = retrySched;
        this.tracingService = tracingService;
    }

    /**
     * Process a batch of events for a topic.
     * Messages are acknowledged only after successful processing.
     *
     * @param pjp The proceeding join point
     * @param listener The listener configuration
     * @param kafkaTemplate The Kafka template for DLQ/retry
     * @return null (batch processing doesn't return a value)
     */
    public Object processBatch(ProceedingJoinPoint pjp,
                               DameroKafkaListener listener,
                               KafkaTemplate<?, ?> kafkaTemplate) throws Throwable {
        String topic = listener.topic();
        long batchStartTime = System.currentTimeMillis();

        // In fixed window mode, only drain up to batchCapacity
        int maxItems = listener.fixedWindow() ? listener.batchCapacity() : 0;
        ConcurrentLinkedQueue<Object[]> batchQueue = batchOrchestrator.drainBatch(topic, maxItems, true);

        if (batchQueue == null || batchQueue.isEmpty()) {
            logger.debug("Batch queue empty for topic: {} - nothing to process", topic);
            return null;
        }

        int processed = 0;
        int failed = 0;
        int batchSize = batchQueue.size();

        // Collect acknowledgments - ack only after successful processing
        List<Acknowledgment> batchAcknowledgments = new ArrayList<>(batchSize);

        logger.info("Processing batch of {} events for topic: {} (fixedWindow: {}, maxCapacity: {})",
            batchSize, topic, listener.fixedWindow(), listener.batchCapacity());

        TracingSpan batchSpan = null;
        TracingScope batchScope = null;
        if (listener.openTelemetry()) {
            batchSpan = tracingService.startProcessingSpan("damero.kafka.batch", topic, null);
            batchSpan.setAttribute("damero.batch.size", batchSize);
            batchSpan.setAttribute("damero.batch.topic", topic);
            batchSpan.setAttribute("damero.batch.fixed_window", listener.fixedWindow());
            batchScope = tracingService.extractContext(null).with(batchSpan);
        }

        try {
            Object[] eventArgs;
            while ((eventArgs = batchQueue.poll()) != null) {
                // Extract acknowledgment for deferred ack
                Acknowledgment ack = extractAcknowledgment(eventArgs);
                if (ack != null) {
                    batchAcknowledgments.add(ack);
                }

                try {
                    processEvent(pjp, listener, kafkaTemplate, eventArgs);
                    processed++;
                } catch (Exception e) {
                    failed++;
                    logger.error("Batch item failed for topic: {}: {}", topic, e.getMessage());
                }
            }

            if (listener.openTelemetry() && batchSpan != null) {
                batchSpan.setAttribute("damero.batch.processed", processed);
                batchSpan.setAttribute("damero.batch.failed", failed);
                batchSpan.setAttribute("damero.outcome", failed == 0 ? "success" : "partial_failure");
                if (failed == 0) {
                    batchSpan.setSuccess();
                }
            }

            // Acknowledge all messages AFTER successful processing
            logger.debug("Acknowledging {} messages after batch processing for topic: {}",
                batchAcknowledgments.size(), topic);
            for (Acknowledgment ack : batchAcknowledgments) {
                try {
                    ack.acknowledge();
                } catch (Exception e) {
                    logger.warn("Failed to acknowledge message in batch for topic: {}: {}",
                        topic, e.getMessage());
                }
            }

        } finally {
            if (listener.openTelemetry() && batchSpan != null) {
                batchSpan.end();
                if (batchScope != null) {
                    batchScope.close();
                }
            }

            // Record batch metrics
            long batchProcessingTime = System.currentTimeMillis() - batchStartTime;
            metricsRecorder.recordBatch(topic, batchSize, processed, failed,
                                       batchProcessingTime, listener.fixedWindow());

            // Schedule next window if messages arrived during processing
            batchOrchestrator.scheduleNextWindowIfNeeded(topic, listener);
        }

        logger.info("Batch complete for topic: {} - processed: {}, failed: {}, time: {}ms",
                   topic, processed, failed, System.currentTimeMillis() - batchStartTime);
        return null;
    }

    /**
     * Check if there are pending messages for subsequent batch windows.
     */
    public boolean hasPendingMessages(String topic) {
        return batchOrchestrator.hasPendingMessages(topic);
    }

    /**
     * Process a single event within a batch.
     */
    private void processEvent(ProceedingJoinPoint pjp,
                              DameroKafkaListener listener,
                              KafkaTemplate<?, ?> kafkaTemplate,
                              Object[] args) throws Throwable {
        long processingStartTime = System.currentTimeMillis();

        Object arg0 = args.length > 0 ? args[0] : null;
        ConsumerRecord<?, ?> consumerRecord = extractConsumerRecord(arg0);
        EventMetadata existingMetadata = null;
        if (consumerRecord != null) {
            existingMetadata = HeaderUtils.extractMetadataFromHeaders(consumerRecord.headers());
        }

        Object event = EventUnwrapper.unwrapEvent(arg0);

        if (event == null) {
            logger.warn("No event found in batch item for topic: {}", listener.topic());
            return;
        }

        Object originalEvent = EventUnwrapper.extractOriginalEvent(event);
        String eventId = EventUnwrapper.extractEventId(originalEvent);

        TracingSpan eventSpan = null;
        TracingScope eventScope = null;
        if (listener.openTelemetry()) {
            eventSpan = tracingService.startProcessingSpan("damero.kafka.batch.item", listener.topic(), eventId);
            eventSpan.setAttribute("damero.event.id", eventId != null ? eventId : "unknown");
            eventScope = tracingService.extractContext(null).with(eventSpan);
        }

        try {
            // Circuit breaker check
            Object circuitBreaker = circuitBreakerWrapper.getCircuitBreaker(listener);
            if (circuitBreaker != null && circuitBreakerWrapper.isOpen(circuitBreaker)) {
                logger.warn("Circuit breaker open for topic: {} - sending to DLQ", listener.topic());
                dlqRouter.sendToDLQForCircuitBreakerOpen(kafkaTemplate, originalEvent, listener);
                if (listener.openTelemetry() && eventSpan != null) {
                    eventSpan.setAttribute("damero.outcome", "dlq_circuit_breaker");
                    eventSpan.setSuccess();
                }
                return;
            }

            // Deduplication check
            if (listener.deDuplication() && duplicationManager.isDuplicate(eventId)) {
                logger.debug("Batch: duplicate detected for eventId: {} - skipping", eventId);
                if (listener.openTelemetry() && eventSpan != null) {
                    eventSpan.setAttribute("damero.outcome", "duplicate_ignored");
                    eventSpan.setSuccess();
                }
                return;
            }

            // Execute listener
            if (circuitBreaker != null) {
                circuitBreakerWrapper.executeWithArgs(circuitBreaker, pjp, args);
            } else {
                pjp.proceed(args);
            }

            metricsRecorder.recordSuccess(listener.topic(), processingStartTime);

            if (listener.deDuplication()) {
                duplicationManager.markAsSeen(eventId);
            }

            retryOrchestrator.clearAttempts(eventId);
            retrySched.clearFibonacciState(event);

            if (listener.openTelemetry() && eventSpan != null) {
                eventSpan.setAttribute("damero.outcome", "success");
                eventSpan.setSuccess();
            }

        } catch (Exception e) {
            if (listener.openTelemetry() && eventSpan != null) {
                eventSpan.setAttribute("damero.outcome", "exception");
                eventSpan.setAttribute("damero.exception.type", e.getClass().getSimpleName());
                eventSpan.recordException(e);
            }
            handleBatchItemException(listener, kafkaTemplate, originalEvent, eventId, e, existingMetadata, processingStartTime);
        } finally {
            if (listener.openTelemetry() && eventSpan != null) {
                eventSpan.end();
                if (eventScope != null) {
                    eventScope.close();
                }
            }
        }
    }



    /**
     * Handle exception for a batch item - retry or send to DLQ.
     */
    private void handleBatchItemException(DameroKafkaListener listener,
                                          KafkaTemplate<?, ?> kafkaTemplate,
                                          Object originalEvent,
                                          String eventId,
                                          Exception e,
                                          EventMetadata existingMetadata,
                                          long processingStartTime) {

        if (!listener.retryable()) {
            metricsRecorder.recordFailure(listener.topic(), e, 1, processingStartTime);
            dlqRouter.sendToDLQAfterMaxAttempts(kafkaTemplate, originalEvent, e, 1, existingMetadata, listener);
            return;
        }

        // Check for non-retryable exceptions
        for (Class<? extends Throwable> nonRetryable : listener.nonRetryableExceptions()) {
            if (nonRetryable.isAssignableFrom(e.getClass())) {
                logger.info("Exception {} is non-retryable for topic: {} - sending to DLQ",
                    e.getClass().getSimpleName(), listener.topic());
                metricsRecorder.recordFailure(listener.topic(), e, 1, processingStartTime);
                dlqRouter.sendToDLQAfterMaxAttempts(kafkaTemplate, originalEvent, e, 1, existingMetadata, listener);
                return;
            }
        }

        int currentAttempt = retryOrchestrator.incrementAttempts(eventId);

        if (currentAttempt >= listener.maxAttempts()) {
            logger.info("Max attempts reached ({}) for event in topic: {} - sending to DLQ",
                currentAttempt, listener.topic());
            metricsRecorder.recordFailure(listener.topic(), e, currentAttempt, processingStartTime);
            dlqRouter.sendToDLQAfterMaxAttempts(kafkaTemplate, originalEvent, e, currentAttempt, existingMetadata, listener);
            retryOrchestrator.clearAttempts(eventId);
        } else {
            logger.debug("Scheduling retry attempt {} for event {} in topic: {}",
                currentAttempt, eventId, listener.topic());
            retryOrchestrator.scheduleRetry(listener, originalEvent, e, currentAttempt, existingMetadata, kafkaTemplate);
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

    private ConsumerRecord<?, ?> extractConsumerRecord(Object arg) {
        if (arg instanceof ConsumerRecord<?, ?>) {
            return (ConsumerRecord<?, ?>) arg;
        }
        return null;
    }
}

