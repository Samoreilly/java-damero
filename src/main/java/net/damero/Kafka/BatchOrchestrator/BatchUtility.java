package net.damero.Kafka.BatchOrchestrator;

import net.damero.Kafka.Annotations.DameroKafkaListener;
import net.damero.Kafka.Aspect.Components.Utility.AspectHelperMethods;
import org.aspectj.lang.ProceedingJoinPoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class BatchUtility {

    private static final Logger logger = LoggerFactory.getLogger(BatchUtility.class);
    private final BatchProcessor batchProcessor;

    public BatchUtility(BatchProcessor batchProcessor) {
        this.batchProcessor = batchProcessor;
    }

    /**
     * Handle batch window expiry callback from BatchOrchestrator.
     * Called asynchronously when the batch window timer expires.
     */
    public BatchCheckResult checkBatchStatus(BatchStatus status, ApplicationContext context,
            KafkaTemplate<?, ?> defaultKafkaTemplate, ProceedingJoinPoint pjp, DameroKafkaListener dameroKafkaListener,
            ConcurrentHashMap<String, ProceedingJoinPoint> topicJoinPoints, String topic) throws Throwable {

        if (status == BatchStatus.PROCESSING) {
            // Still collecting - DO NOT acknowledge yet
            // Messages will be redelivered if server crashes before batch completes
            // Deduplication will handle any reprocessing after recovery
            logger.debug("Batch collecting for topic: {} - message queued (unacknowledged)", topic);
            return new BatchCheckResult(BatchStatus.PROCESSING, null);

        }

        if (status == BatchStatus.CAPACITY_REACHED) {
            // Capacity reached - process the batch immediately
            logger.debug("Batch capacity reached for topic: {} - processing batch", topic);
            KafkaTemplate<?, ?> kafkaTemplate = AspectHelperMethods.resolveKafkaTemplate(dameroKafkaListener, context,
                    defaultKafkaTemplate);
            Object result = batchProcessor.processBatch(pjp, dameroKafkaListener, kafkaTemplate);

            // Clean up join point if no more pending messages
            if (!batchProcessor.hasPendingMessages(topic)) {
                topicJoinPoints.remove(topic);
            }
            return new BatchCheckResult(BatchStatus.CAPACITY_REACHED, result);
        }
        return new BatchCheckResult(BatchStatus.CONTINUE_PROCESSING, null);
    }
}
