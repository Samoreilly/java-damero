
package net.damero.CustomKafkaSetup.RetryKafkaListener;

import net.damero.CustomKafkaSetup.DelayMethod;
import net.damero.CustomObject.EventMetadata;
import net.damero.CustomObject.EventWrapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class KafkaRetryListener {


    private KafkaTemplate<String, Object> defaultKafkaTemplate;

    public KafkaRetryListener(KafkaTemplate<String, Object> defaultKafkaTemplate,
                              RetryDelayCalculator delayCalculator) {
        this.defaultKafkaTemplate = defaultKafkaTemplate;
        this.delayCalculator = delayCalculator;
    }

    @Autowired
    private RetryDelayCalculator delayCalculator;

    private ScheduledExecutorService scheduler;

    @PostConstruct
    public void init() {
        ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(
                Math.max(4, Runtime.getRuntime().availableProcessors()),
                r -> {
                    Thread thread = new Thread(r);
                    thread.setName("kafka-retry-scheduler-" + thread.getId());
                    thread.setDaemon(true);
                    return thread;
                }
        );
        executor.setRemoveOnCancelPolicy(true);
        executor.setMaximumPoolSize(Runtime.getRuntime().availableProcessors() * 2);
        this.scheduler = executor;

        System.out.println("✅ KafkaRetryListener initialized with " +
                executor.getCorePoolSize() + " scheduler threads (non-blocking)");
    }

    @PreDestroy
    public void destroy() {
        if (scheduler != null && !scheduler.isShutdown()) {
            System.out.println("🛑 Shutting down retry scheduler...");
            scheduler.shutdown();
            try {
                if (!scheduler.awaitTermination(30, TimeUnit.SECONDS)) {
                    System.err.println("⚠️ Scheduler did not terminate in time, forcing shutdown");
                    scheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                scheduler.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * Generic retry listener - listens to topics matching pattern "*-retry"
     * CHANGED: Use retryConsumerFactory instead of defaultFactory
     */


    @KafkaListener(
            topicPattern = ".*-retry",
            groupId = "kafka-retry-group",
            containerFactory = "retryContainerFactory"
    )
    public void onMessage(ConsumerRecord<String, EventWrapper<?>> record) {
        System.out.println("📥 KafkaRetryListener received message from topic: " + record.topic());

        EventWrapper<?> eventWrapper = record.value();

        if (eventWrapper == null || eventWrapper.getMetadata() == null) {
            System.err.println("⚠️ Received null event or metadata in retry topic");
            return;
        }

        EventMetadata metadata = eventWrapper.getMetadata();
        String originalTopic = metadata.getOriginalTopic();
        int attemptNumber = metadata.getAttempts();
        int maxAttempts = metadata.getMaxAttempts() != null ? metadata.getMaxAttempts() : 3;

        System.out.println("📥 Retry listener received event (attempt " + attemptNumber +
                "/" + maxAttempts + ") - topic: " + originalTopic);

        // Check if max attempts exceeded - send to DLQ
        if (attemptNumber >= maxAttempts) {
            System.out.println("🚨 Max attempts (" + maxAttempts + ") reached, sending to DLQ");
            sendToDLQ(originalTopic, eventWrapper);
            return;
        }

        // Calculate delay based on metadata configuration
        long delayMillis = calculateDelayFromMetadata(metadata);

        // Schedule non-blocking retry after delay
        scheduler.schedule(
                () -> sendToOriginalTopic(originalTopic, eventWrapper, attemptNumber),
                delayMillis,
                TimeUnit.MILLISECONDS
        );

        System.out.println("⏰ Scheduled retry for topic '" + originalTopic +
                "' after " + delayMillis + "ms (non-blocking, " +
                "active tasks: " + ((ScheduledThreadPoolExecutor) scheduler).getActiveCount() + ")");
    }

    private long calculateDelayFromMetadata(EventMetadata metadata) {
        int attemptNumber = metadata.getAttempts();

        Long baseDelayMs = metadata.getDelayMs();
        DelayMethod delayMethod = metadata.getDelayMethod();

        if (baseDelayMs == null || baseDelayMs <= 0) {
            baseDelayMs = 1000L;
        }

        if (delayMethod == null) {
            delayMethod = DelayMethod.EXPO;
        }

        System.out.println("📊 Calculating delay: method=" + delayMethod +
                ", baseDelay=" + baseDelayMs + "ms, attempt=" + attemptNumber);

        return delayCalculator.calculateDelayWithJitter(delayMethod, baseDelayMs, attemptNumber);
    }

    private void sendToOriginalTopic(String originalTopic, EventWrapper<?> eventWrapper, int attemptNumber) {
        try {
            System.out.println("🔄 Sending event back to original topic: " + originalTopic +
                    " (attempt " + attemptNumber + ")");
            Object originalEvent = eventWrapper.getEvent();
            defaultKafkaTemplate.send(originalTopic, originalEvent)
                    .whenComplete((result, ex) -> {
                        if (ex != null) {
                            System.err.println("❌ Failed to send retry message to " + originalTopic +
                                    ": " + ex.getMessage());
                        } else {
                            System.out.println("✅ Successfully sent retry to " + originalTopic +
                                    " (partition: " + result.getRecordMetadata().partition() +
                                    ", offset: " + result.getRecordMetadata().offset() + ")");
                        }
                    });

        } catch (Exception e) {
            System.err.println("❌ Exception while sending to original topic " + originalTopic +
                    ": " + e.getMessage());
            e.printStackTrace();
        }
    }

    private void sendToDLQ(String originalTopic, EventWrapper<?> eventWrapper) {
        try {
            String dlqTopic = originalTopic + "-dlq";

            System.out.println("💀 Sending event to DLQ: " + dlqTopic +
                    " (attempts: " + eventWrapper.getMetadata().getAttempts() + ")");

            defaultKafkaTemplate.send(dlqTopic, eventWrapper)
                    .whenComplete((result, ex) -> {
                        if (ex != null) {
                            System.err.println("❌ CRITICAL: Failed to send to DLQ " + dlqTopic +
                                    ": " + ex.getMessage());
                        } else {
                            System.out.println("✅ Successfully sent to DLQ " + dlqTopic +
                                    " (partition: " + result.getRecordMetadata().partition() +
                                    ", offset: " + result.getRecordMetadata().offset() + ")");
                        }
                    });

        } catch (Exception e) {
            System.err.println("❌ CRITICAL: Exception while sending to DLQ: " + e.getMessage());
            e.printStackTrace();
        }
    }

    public SchedulerStats getStats() {
        if (scheduler instanceof ScheduledThreadPoolExecutor) {
            ScheduledThreadPoolExecutor exec = (ScheduledThreadPoolExecutor) scheduler;
            return new SchedulerStats(
                    exec.getActiveCount(),
                    exec.getQueue().size(),
                    exec.getCompletedTaskCount()
            );
        }
        return new SchedulerStats(0, 0, 0);
    }

    public static class SchedulerStats {
        private final int activeThreads;
        private final int queuedTasks;
        private final long completedTasks;

        public SchedulerStats(int activeThreads, int queuedTasks, long completedTasks) {
            this.activeThreads = activeThreads;
            this.queuedTasks = queuedTasks;
            this.completedTasks = completedTasks;
        }

        public int getActiveThreads() { return activeThreads; }
        public int getQueuedTasks() { return queuedTasks; }
        public long getCompletedTasks() { return completedTasks; }

        @Override
        public String toString() {
            return String.format("SchedulerStats{active=%d, queued=%d, completed=%d}",
                    activeThreads, queuedTasks, completedTasks);
        }
    }
}