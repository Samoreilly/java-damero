package net.damero.Kafka.BatchOrchestrator;

import net.damero.Kafka.Annotations.DameroKafkaListener;
import net.damero.Kafka.Aspect.Components.*;
import net.damero.Kafka.Aspect.Components.Utility.EventUnwrapper;
import net.damero.Kafka.Aspect.Components.Utility.HeaderUtils;
import net.damero.Kafka.Aspect.Components.Utility.MetricsRecorder;
import net.damero.Kafka.Aspect.Deduplication.DuplicationManager;
import net.damero.Kafka.CustomObject.EventMetadata;
import net.damero.Kafka.RetryScheduler.RetrySched;
import net.damero.Kafka.Tracing.TracingContext;
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

    public Object processBatch(ProceedingJoinPoint pjp,
            DameroKafkaListener listener,
            KafkaTemplate<?, ?> kafkaTemplate) throws Throwable {
        String topic = listener.topic();
        long batchStartTime = System.currentTimeMillis();
        int maxItems = listener.fixedWindow() ? listener.batchCapacity() : 0;
        ConcurrentLinkedQueue<Object[]> batchQueue = batchOrchestrator.drainBatch(topic, maxItems, true);

        if (batchQueue == null || batchQueue.isEmpty())
            return null;

        int processed = 0, failed = 0, batchSize = batchQueue.size();
        List<Acknowledgment> batchAcknowledgments = new ArrayList<>(batchSize);

        TracingSpan batchSpan = null;
        TracingScope batchScope = null;
        if (listener.openTelemetry()) {
            batchSpan = tracingService.startProcessingSpan("damero.kafka.batch", topic, null);
            batchSpan.setAttribute("damero.batch.size", batchSize);
            // Activate the span so it becomes the parent for item spans
            TracingContext currentContext = tracingService.extractContext(null);
            batchScope = currentContext.with(batchSpan);
        }

        try {
            Object[] eventArgs;
            while ((eventArgs = batchQueue.poll()) != null) {
                Acknowledgment ack = extractAcknowledgment(eventArgs);
                if (ack != null)
                    batchAcknowledgments.add(ack);
                try {
                    processEvent(pjp, listener, kafkaTemplate, eventArgs);
                    processed++;
                } catch (Exception e) {
                    failed++;
                    logger.error("Batch item failed: {}", e.getMessage());
                }
            }

            for (Acknowledgment ack : batchAcknowledgments) {
                try {
                    ack.acknowledge();
                } catch (Exception ignored) {
                }
            }
        } finally {
            if (batchScope != null) {
                batchScope.close();
            }
            if (batchSpan != null) {
                batchSpan.setAttribute("damero.batch.processed", processed);
                batchSpan.setAttribute("damero.batch.failed", failed);
                batchSpan.end();
            }
            metricsRecorder.recordBatch(topic, batchSize, processed, failed,
                    System.currentTimeMillis() - batchStartTime, listener.fixedWindow());
            batchOrchestrator.scheduleNextWindowIfNeeded(topic, listener);
        }
        return null;
    }

    private void processEvent(ProceedingJoinPoint pjp, DameroKafkaListener listener, KafkaTemplate<?, ?> kafkaTemplate,
            Object[] args) throws Throwable {
        long startTime = System.currentTimeMillis();
        ConsumerRecord<?, ?> consumerRecord = extractConsumerRecord(args);
        EventMetadata existingMetadata = consumerRecord != null
                ? HeaderUtils.extractMetadataFromHeaders(consumerRecord.headers())
                : null;
        Object event = EventUnwrapper.unwrapEvent(args.length > 0 ? args[0] : null);
        if (event == null)
            return;
        Object originalEvent = EventUnwrapper.extractOriginalEvent(event);
        String eventId = EventUnwrapper.extractEventId(originalEvent, consumerRecord);

        TracingSpan eventSpan = null;
        if (listener.openTelemetry()) {
            eventSpan = tracingService.startProcessingSpan("damero.kafka.batch.item", listener.topic(), eventId);
        }

        try {
            Object cb = circuitBreakerWrapper.getCircuitBreaker(listener);
            if (cb != null && circuitBreakerWrapper.isOpen(cb)) {
                dlqRouter.sendToDLQForCircuitBreakerOpen(kafkaTemplate, originalEvent, listener, eventId);
                return;
            }
            if (listener.deDuplication() && duplicationManager.isDuplicate(eventId))
                return;

            if (cb != null)
                circuitBreakerWrapper.executeWithArgs(cb, pjp, args);
            else
                pjp.proceed(args);

            metricsRecorder.recordSuccess(listener.topic(), startTime);
            if (listener.deDuplication())
                duplicationManager.markAsSeen(eventId);
            retryOrchestrator.clearAttempts(eventId);
            retrySched.clearFibonacciState(event);
            if (eventSpan != null)
                eventSpan.setSuccess();
        } catch (Exception e) {
            handleBatchItemException(listener, kafkaTemplate, originalEvent, eventId, e, existingMetadata, startTime);
        } finally {
            if (eventSpan != null)
                eventSpan.end();
        }
    }

    private void handleBatchItemException(DameroKafkaListener listener, KafkaTemplate<?, ?> kafkaTemplate,
            Object originalEvent, String eventId, Exception e, EventMetadata metadata, long start) {
        if (!listener.retryable() || isNonRetryable(listener, e)) {
            metricsRecorder.recordFailure(listener.topic(), e, 1, start);
            dlqRouter.sendToDLQAfterMaxAttempts(kafkaTemplate, originalEvent, e, 1, metadata, eventId, listener);
            return;
        }
        int attempt = retryOrchestrator.incrementAttempts(eventId);
        if (attempt >= listener.maxAttempts()) {
            metricsRecorder.recordFailure(listener.topic(), e, attempt, start);
            dlqRouter.sendToDLQAfterMaxAttempts(kafkaTemplate, originalEvent, e, attempt, metadata, eventId, listener);
            retryOrchestrator.clearAttempts(eventId);
        } else {
            retryOrchestrator.scheduleRetry(listener, eventId, originalEvent, e, attempt, metadata, kafkaTemplate);
        }
    }

    private boolean isNonRetryable(DameroKafkaListener listener, Exception e) {
        for (Class<? extends Throwable> nr : listener.nonRetryableExceptions()) {
            if (nr.isAssignableFrom(e.getClass()))
                return true;
        }
        return false;
    }

    public boolean hasPendingMessages(String topic) {
        return batchOrchestrator.hasPendingMessages(topic);
    }

    private Acknowledgment extractAcknowledgment(Object[] args) {
        for (Object arg : args)
            if (arg instanceof Acknowledgment)
                return (Acknowledgment) arg;
        return null;
    }

    private ConsumerRecord<?, ?> extractConsumerRecord(Object[] args) {
        for (Object arg : args)
            if (arg instanceof ConsumerRecord<?, ?>)
                return (ConsumerRecord<?, ?>) arg;
        return null;
    }
}
