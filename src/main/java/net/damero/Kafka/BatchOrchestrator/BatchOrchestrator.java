package net.damero.Kafka.BatchOrchestrator;

import net.damero.Kafka.Annotations.CustomKafkaListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;

/**
 * Orchestrates batch collection and processing for Kafka messages.
 * Supports both capacity-based and time-window-based batch triggering.
 *
 * <p>Thread-safety: Uses per-topic locks to prevent race conditions between
 * capacity-triggered and window-triggered batch processing.</p>
 */
@Component
public class BatchOrchestrator {

    private static final Logger logger = LoggerFactory.getLogger(BatchOrchestrator.class);

    private final TaskScheduler taskScheduler;

    // Core batch state per topic
    private final ConcurrentHashMap<String, ConcurrentLinkedQueue<Object[]>> batchQueues = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, AtomicLong> batchCounters = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Long> batchWindowStartTimes = new ConcurrentHashMap<>();

    // Scheduled window expiry tasks per topic
    private final ConcurrentHashMap<String, ScheduledFuture<?>> windowExpiryTasks = new ConcurrentHashMap<>();

    // Per-topic locks to prevent race conditions during drain
    private final ConcurrentHashMap<String, ReentrantLock> topicLocks = new ConcurrentHashMap<>();

    // Flag to prevent double-processing (capacity vs window race)
    private final ConcurrentHashMap<String, AtomicBoolean> processingFlags = new ConcurrentHashMap<>();

    // Callback for when window expires (set by KafkaListenerAspect)
    private volatile BiConsumer<String, CustomKafkaListener> windowExpiryCallback;

    public BatchOrchestrator(@Qualifier("kafkaRetryScheduler") TaskScheduler taskScheduler) {
        this.taskScheduler = taskScheduler;
    }

    /**
     * Register a callback to be invoked when a batch window expires.
     */
    public void setWindowExpiryCallback(BiConsumer<String, CustomKafkaListener> callback) {
        this.windowExpiryCallback = callback;
    }

    /**
     * Orchestrates batch collection for a given topic.
     * Thread-safe: uses per-topic locking to prevent race conditions.
     *
     * @param listener The listener annotation with batch config
     * @param topic The topic being consumed
     * @param args The full argument array from the listener method
     * @return BatchStatus indicating whether to continue collecting or process
     */
    public BatchStatus orchestrate(CustomKafkaListener listener, String topic, Object[] args) {
        ReentrantLock lock = topicLocks.computeIfAbsent(topic, k -> new ReentrantLock());
        lock.lock();
        try {
            // Check if already being processed (window expiry beat us)
            AtomicBoolean processing = processingFlags.get(topic);
            if (processing != null && processing.get()) {
                // Another thread is processing - queue for next batch
                ConcurrentLinkedQueue<Object[]> queue = batchQueues.computeIfAbsent(topic, k -> new ConcurrentLinkedQueue<>());
                queue.add(args);
                return BatchStatus.PROCESSING;
            }

            // Add to queue
            ConcurrentLinkedQueue<Object[]> queue = batchQueues.computeIfAbsent(topic, k -> new ConcurrentLinkedQueue<>());
            queue.add(args);

            // Increment counter
            long currentCount = batchCounters.computeIfAbsent(topic, k -> new AtomicLong(0)).incrementAndGet();

            // Start window timer on first message
            boolean isFirstMessage = batchWindowStartTimes.putIfAbsent(topic, System.currentTimeMillis()) == null;
            if (isFirstMessage) {
                scheduleWindowExpiry(topic, listener);
            }

            int batchCapacity = listener.batchCapacity();
            int minimumCapacity = listener.minimumCapacity();

            logger.debug("Batch for topic: {}, count: {}/{}", topic, currentCount, batchCapacity);

            // Check capacity
            if (currentCount >= batchCapacity) {
                // Set processing flag to prevent window callback from also processing
                processingFlags.computeIfAbsent(topic, k -> new AtomicBoolean(false)).set(true);
                cancelWindowExpiryTask(topic);
                logger.info("Batch capacity reached for topic: {} ({}/{})", topic, currentCount, batchCapacity);
                return BatchStatus.CAPACITY_REACHED;
            }

            // Check minimum capacity threshold (optional early trigger)
            if (minimumCapacity > 0 && currentCount >= minimumCapacity) {
                logger.debug("Minimum capacity threshold reached for topic: {} ({}/{})",
                    topic, currentCount, minimumCapacity);
                // Could add MINIMUM_REACHED status if needed for future use
            }

            return BatchStatus.PROCESSING;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Schedule a task to trigger batch processing when the window expires.
     */
    private void scheduleWindowExpiry(String topic, CustomKafkaListener listener) {
        int windowLengthMs = listener.batchWindowLength();

        ScheduledFuture<?> task = taskScheduler.schedule(
            () -> handleWindowExpiry(topic, listener),
            Instant.now().plusMillis(windowLengthMs)
        );

        // Cancel any existing task and store the new one
        ScheduledFuture<?> existingTask = windowExpiryTasks.put(topic, task);
        if (existingTask != null && !existingTask.isDone()) {
            existingTask.cancel(false);
        }

        logger.debug("Scheduled batch window expiry for topic: {} in {}ms", topic, windowLengthMs);
    }

    /**
     * Handle window expiry - invoke callback if there are pending messages.
     * Thread-safe: checks processing flag to prevent double-processing.
     */
    private void handleWindowExpiry(String topic, CustomKafkaListener listener) {
        ReentrantLock lock = topicLocks.computeIfAbsent(topic, k -> new ReentrantLock());
        lock.lock();
        try {
            windowExpiryTasks.remove(topic);

            // Check if capacity-triggered processing already handled this batch
            AtomicBoolean processing = processingFlags.get(topic);
            if (processing != null && processing.get()) {
                logger.debug("Batch for topic: {} already being processed by capacity trigger", topic);
                return;
            }

            if (!hasPendingMessages(topic)) {
                logger.debug("Batch window expired for topic: {} but no pending messages", topic);
                return;
            }

            // Set processing flag
            processingFlags.computeIfAbsent(topic, k -> new AtomicBoolean(false)).set(true);

            logger.info("Batch window expired for topic: {} - triggering processing ({} messages)",
                topic, getBatchCount(topic));

        } finally {
            lock.unlock();
        }

        // Invoke callback outside of lock to prevent deadlock
        if (windowExpiryCallback != null) {
            windowExpiryCallback.accept(topic, listener);
        } else {
            logger.warn("No window expiry callback registered for topic: {}", topic);
        }
    }

    /**
     * Cancel the window expiry task for a topic.
     */
    private void cancelWindowExpiryTask(String topic) {
        ScheduledFuture<?> task = windowExpiryTasks.remove(topic);
        if (task != null && !task.isDone()) {
            task.cancel(false);
            logger.debug("Cancelled window expiry task for topic: {}", topic);
        }
    }

    /**
     * Drain and return the batch queue for a topic, resetting all state atomically.
     * Thread-safe: uses per-topic locking.
     */
    public ConcurrentLinkedQueue<Object[]> drainBatch(String topic) {
        ReentrantLock lock = topicLocks.computeIfAbsent(topic, k -> new ReentrantLock());
        lock.lock();
        try {
            ConcurrentLinkedQueue<Object[]> queue = batchQueues.remove(topic);

            AtomicLong counter = batchCounters.get(topic);
            if (counter != null) {
                counter.set(0);
            }

            batchWindowStartTimes.remove(topic);
            cancelWindowExpiryTask(topic);

            // Reset processing flag for next batch
            AtomicBoolean processing = processingFlags.get(topic);
            if (processing != null) {
                processing.set(false);
            }

            return queue;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Get current batch count for a topic.
     */
    public long getBatchCount(String topic) {
        AtomicLong counter = batchCounters.get(topic);
        return counter != null ? counter.get() : 0;
    }

    /**
     * Check if there are pending messages in the batch queue.
     */
    public boolean hasPendingMessages(String topic) {
        ConcurrentLinkedQueue<Object[]> queue = batchQueues.get(topic);
        return queue != null && !queue.isEmpty();
    }

    /**
     * Check if a batch window is currently active for a topic.
     */
    public boolean hasActiveWindow(String topic) {
        return batchWindowStartTimes.containsKey(topic);
    }

    /**
     * Check if a batch is currently being processed for a topic.
     * Useful for testing and monitoring.
     */
    public boolean isProcessing(String topic) {
        AtomicBoolean processing = processingFlags.get(topic);
        return processing != null && processing.get();
    }
}
