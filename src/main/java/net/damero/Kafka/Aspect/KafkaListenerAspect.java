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
import net.damero.Kafka.Aspect.Components.Utility.RawBinaryWrapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.reflect.MethodSignature;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationContext;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import net.damero.Kafka.CustomObject.EventMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
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
        this.batchOrchestrator.setWindowExpiryCallback(this::handleBatchWindowExpiry);
    }

    private void handleBatchWindowExpiry(String topic, DameroKafkaListener listener) {
        ProceedingJoinPoint pjp = topicJoinPoints.get(topic);
        if (pjp == null)
            return;
        try {
            KafkaTemplate<?, ?> kafkaTemplate = AspectHelperMethods.resolveKafkaTemplate(listener, context,
                    defaultKafkaTemplate);
            batchProcessor.processBatch(pjp, listener, kafkaTemplate);
            if (!batchProcessor.hasPendingMessages(topic))
                topicJoinPoints.remove(topic);
        } catch (Throwable e) {
            logger.error("Error processing expired batch for topic: {}: {}", topic, e.getMessage());
        }
    }

    @Around("@annotation(dameroKafkaListener)")
    public Object kafkaListener(ProceedingJoinPoint pjp, DameroKafkaListener dameroKafkaListener) throws Throwable {

        if (dameroKafkaListener.batchCapacity() > 0) {
            String topic = dameroKafkaListener.topic();
            topicJoinPoints.putIfAbsent(topic, pjp);
            BatchStatus status = batchOrchestrator.orchestrate(dameroKafkaListener, topic, pjp.getArgs());
            BatchCheckResult batchCheck = batchUtility.checkBatchStatus(status, context, defaultKafkaTemplate, pjp,
                    dameroKafkaListener, topicJoinPoints, topic);
            if (batchCheck.status() == BatchStatus.PROCESSING)
                return null;
            else if (batchCheck.status() == BatchStatus.CAPACITY_REACHED)
                return batchCheck.object();
        }

        long processingStartTime = System.currentTimeMillis();
        Object[] args = pjp.getArgs();
        Acknowledgment acknowledgment = AspectHelperMethods.extractAcknowledgment(args);
        ConsumerRecord<?, ?> consumerRecord = AspectHelperMethods.extractConsumerRecord(args);

        EventMetadata existingMetadata = consumerRecord != null
                ? HeaderUtils.extractMetadataFromHeaders(consumerRecord.headers())
                : null;
        Object event = EventUnwrapper.unwrapEvent(args.length > 0 ? args[0] : null);
        KafkaTemplate<?, ?> kafkaTemplate = AspectHelperMethods.resolveKafkaTemplate(dameroKafkaListener, context,
                defaultKafkaTemplate);

        if (event == null) {
            if (acknowledgment != null)
                acknowledgment.acknowledge();
            return null;
        }

        Object originalEvent = EventUnwrapper.extractOriginalEvent(event);


        String eventId = EventUnwrapper.extractEventId(originalEvent, consumerRecord);

        TracingSpan processingSpan = null;
        TracingScope scope = null;
        if (dameroKafkaListener.openTelemetry()) {
            TracingContext parentContext = consumerRecord != null
                    ? tracingService.extractContext(consumerRecord.headers())
                    : tracingService.extractContext(null);
            processingSpan = tracingService.startProcessingSpan("damero.kafka.process", dameroKafkaListener.topic(),
                    eventId);
            scope = parentContext.with(processingSpan);
        }

        try {
            Object circuitBreaker = circuitBreakerWrapper.getCircuitBreaker(dameroKafkaListener);
            if (circuitBreaker != null && circuitBreakerWrapper.isOpen(circuitBreaker)) {
                dlqRouter.sendToDLQForCircuitBreakerOpen(kafkaTemplate, originalEvent, dameroKafkaListener, eventId);
                if (acknowledgment != null)
                    acknowledgment.acknowledge();
                if (processingSpan != null)
                    processingSpan.setSuccess();
                return null;
            }

            if (dameroKafkaListener.deDuplication() && duplicationManager.isDuplicate(eventId)) {
                if (acknowledgment != null)
                    acknowledgment.acknowledge();
                if (processingSpan != null)
                    processingSpan.setSuccess();
                return null;
            }

            try {
                Object result = (circuitBreaker != null)
                        ? circuitBreakerWrapper.executeWithArgs(circuitBreaker, pjp, args)
                        : pjp.proceed(args);
                metricsRecorder.recordSuccess(dameroKafkaListener.topic(), processingStartTime);
                if (acknowledgment != null)
                    acknowledgment.acknowledge();
                if (dameroKafkaListener.deDuplication())
                    duplicationManager.markAsSeen(eventId);
                retryOrchestrator.clearAttempts(eventId);
                retrySched.clearFibonacciState(event);
                if (processingSpan != null)
                    processingSpan.setSuccess();
                return result;
            } catch (Exception e) {
                if (processingSpan != null)
                    processingSpan.recordException(e);
                if (!dameroKafkaListener.retryable()) {
                    metricsRecorder.recordFailure(dameroKafkaListener.topic(), e, 1, processingStartTime);
                    dlqRouter.sendToDLQAfterMaxAttempts(kafkaTemplate, originalEvent, e, 1, existingMetadata, eventId,
                            dameroKafkaListener);
                    return null;
                }
                if (acknowledgment != null)
                    acknowledgment.acknowledge();
                if (dameroKafkaListener.dlqRoutes().length > 0) {
                    if (dlqExceptionRoutingManager.routeToDLQIfSkipRetry(dameroKafkaListener, kafkaTemplate,
                            originalEvent, e, existingMetadata, eventId)) {
                        metricsRecorder.recordFailure(dameroKafkaListener.topic(), e, 1, processingStartTime);
                        return null;
                    }
                }
                if (AspectHelperMethods.isNonRetryableException(e, dameroKafkaListener)) {
                    metricsRecorder.recordFailure(dameroKafkaListener.topic(), e, 1, processingStartTime);
                    dlqRouter.sendToDLQAfterMaxAttempts(kafkaTemplate, originalEvent, e, 1, existingMetadata, eventId,
                            dameroKafkaListener);
                    return null;
                }
                int currentAttempts = retryOrchestrator.incrementAttempts(eventId);
                metricsRecorder.recordFailure(dameroKafkaListener.topic(), e, currentAttempts, processingStartTime);
                if (retryOrchestrator.hasReachedMaxAttempts(eventId, dameroKafkaListener.maxAttempts())) {
                    if (dameroKafkaListener.dlqRoutes().length > 0) {
                        dlqExceptionRoutingManager.routeToDLQAfterMaxAttempts(dameroKafkaListener, kafkaTemplate,
                                originalEvent, e, eventId, currentAttempts, existingMetadata);
                    } else {
                        dlqRouter.sendToDLQAfterMaxAttempts(kafkaTemplate, originalEvent, e, currentAttempts,
                                existingMetadata, eventId, dameroKafkaListener);
                        retryOrchestrator.clearAttempts(eventId);
                    }
                    return null;
                }
                retryOrchestrator.scheduleRetry(dameroKafkaListener, eventId, originalEvent, e, currentAttempts,
                        existingMetadata, kafkaTemplate);
                return null;
            }
        } finally {
            if (scope != null)
                scope.close();
            if (processingSpan != null)
                processingSpan.end();
        }
    }
}
