package net.damero.Kafka.BatchOrchestrator;

import net.damero.Kafka.Annotations.DameroKafkaListener;
import net.damero.Kafka.Aspect.Components.Utility.MetricsRecorder;
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
    private final MetricsRecorder metricsRecorder;

    // Core batch state per topic
    private final ConcurrentHashMap<String, ConcurrentLinkedQueue<Object[]>> batchQueues = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, AtomicLong> batchCounters = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Long> batchWindowStartTimes = new ConcurrentHashMap<>();

    // Track last batch processing time for fixed window spacing
    private final ConcurrentHashMap<String, Long> lastBatchProcessedTimes = new ConcurrentHashMap<>();

    // Scheduled window expiry tasks per topic
    private final ConcurrentHashMap<String, ScheduledFuture<?>> windowExpiryTasks = new ConcurrentHashMap<>();

    // Per-topic locks to prevent race conditions during drain
    private final ConcurrentHashMap<String, ReentrantLock> topicLocks = new ConcurrentHashMap<>();

    // Flag to prevent double-processing (capacity vs window race)
    private final ConcurrentHashMap<String, AtomicBoolean> processingFlags = new ConcurrentHashMap<>();

    // Callback for when window expires (set by KafkaListenerAspect)
    private volatile BiConsumer<String, DameroKafkaListener> windowExpiryCallback;

    public BatchOrchestrator(@Qualifier("kafkaRetryScheduler") TaskScheduler taskScheduler,
                            MetricsRecorder metricsRecorder) {
        this.taskScheduler = taskScheduler;
        this.metricsRecorder = metricsRecorder;
    }

    /**
     * Register a callback to be invoked when a batch window expires.
     */
    public void setWindowExpiryCallback(BiConsumer<String, DameroKafkaListener> callback) {
        this.windowExpiryCallback = callback;
    }

    /**
     * Orchestrates batch collection for a given topic.
     * Thread-safe: uses per-topic locking to prevent race conditions.
     *
     * <p>Supports two modes:</p>
     * <ul>
     *   <li>Capacity-First (fixedWindow=false): Process immediately when capacity reached</li>
     *   <li>Fixed Window (fixedWindow=true): Process only when window timer expires</li>
     * </ul>
     *
     * @param listener The listener annotation with batch config
     * @param topic The topic being consumed
     * @param args The full argument array from the listener method
     * @return BatchStatus indicating whether to continue collecting or process
     */
    public BatchStatus orchestrate(DameroKafkaListener listener, String topic, Object[] args) {
        ReentrantLock lock = topicLocks.computeIfAbsent(topic, k -> new ReentrantLock());
        lock.lock();
        try {
            // Check if already being processed
            AtomicBoolean processing = processingFlags.get(topic);
            if (processing != null && processing.get()) {
                // Another thread is processing - queue for NEXT batch
                ConcurrentLinkedQueue<Object[]> queue = batchQueues.computeIfAbsent(topic, k -> new ConcurrentLinkedQueue<>());
                queue.add(args);
                batchCounters.computeIfAbsent(topic, k -> new AtomicLong(0)).incrementAndGet();

                // Mark that we need to schedule a new window after processing completes
                // This is handled in drainBatch -> the next message will trigger window scheduling
                logger.debug("Message queued during batch processing for topic: {} - will be included in next batch", topic);
                return BatchStatus.PROCESSING;
            }

            int batchCapacity = listener.batchCapacity();
            boolean fixedWindow = listener.fixedWindow();

            // In fixed window mode, enforce capacity as a hard limit
            ConcurrentLinkedQueue<Object[]> queue = batchQueues.computeIfAbsent(topic, k -> new ConcurrentLinkedQueue<>());
            long currentCount = batchCounters.computeIfAbsent(topic, k -> new AtomicLong(0)).get();

            // In fixed window mode, we still queue messages even beyond capacity
            // This ensures all messages in the poll batch are captured
            // The batch processor will drain only batchCapacity items at a time
            // Remaining items will be processed in subsequent batches

            // Add to queue
            queue.add(args);
            currentCount = batchCounters.get(topic).incrementAndGet();

            // Start window timer on first message (or if no active window)
            boolean needsWindowSchedule = batchWindowStartTimes.putIfAbsent(topic, System.currentTimeMillis()) == null;
            if (needsWindowSchedule && windowExpiryTasks.get(topic) == null) {
                scheduleWindowExpiry(topic, listener);
            }

            int minimumCapacity = listener.minimumCapacity();

            logger.debug("Batch for topic: {}, count: {}/{}, fixedWindow: {}", topic, currentCount, batchCapacity, fixedWindow);

            // FIXED WINDOW MODE: Only process when window expires, capacity is just a limit
            if (fixedWindow) {
                return BatchStatus.PROCESSING;
            }

            // CAPACITY-FIRST MODE (default): Process immediately when capacity reached
            if (currentCount >= batchCapacity) {
                // Set processing flag to prevent window callback from also processing
                processingFlags.computeIfAbsent(topic, k -> new AtomicBoolean(false)).set(true);
                cancelWindowExpiryTask(topic);
                logger.info("Batch capacity reached for topic: {} ({}/{})", topic, currentCount, batchCapacity);
                metricsRecorder.recordBatchCapacityReached(topic);
                return BatchStatus.CAPACITY_REACHED;
            }

            // Check minimum capacity threshold (optional early trigger, only in capacity-first mode)
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
     * In fixed window mode, ensures proper spacing from last batch processing.
     */
    private void scheduleWindowExpiry(String topic, DameroKafkaListener listener) {
        int windowLengthMs = listener.batchWindowLength();
        boolean fixedWindow = listener.fixedWindow();

        long delayMs = windowLengthMs;

        // In fixed window mode, calculate delay from last batch processing time
        // to ensure consistent spacing between batches
        if (fixedWindow) {
            Long lastProcessedTime = lastBatchProcessedTimes.get(topic);
            if (lastProcessedTime != null) {
                long timeSinceLastBatch = System.currentTimeMillis() - lastProcessedTime;
                if (timeSinceLastBatch < windowLengthMs) {
                    // Not enough time has passed, schedule for remaining time
                    delayMs = windowLengthMs - timeSinceLastBatch;
                    logger.debug("Fixed window: {}ms since last batch, scheduling in {}ms for topic: {}",
                        timeSinceLastBatch, delayMs, topic);
                }
            }
        }

        ScheduledFuture<?> task = taskScheduler.schedule(
            () -> handleWindowExpiry(topic, listener),
            Instant.now().plusMillis(delayMs)
        );

        // Cancel any existing task and store the new one
        ScheduledFuture<?> existingTask = windowExpiryTasks.put(topic, task);
        if (existingTask != null && !existingTask.isDone()) {
            existingTask.cancel(false);
        }

        logger.debug("Scheduled batch window expiry for topic: {} in {}ms (fixedWindow: {})", topic, delayMs, fixedWindow);
    }

    /**
     * Handle window expiry - invoke callback if there are pending messages.
     * Thread-safe: checks processing flag to prevent double-processing.
     */
    private void handleWindowExpiry(String topic, DameroKafkaListener listener) {
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

            long messageCount = getBatchCount(topic);
            logger.info("Batch window expired for topic: {} - triggering processing ({} messages)",
                topic, messageCount);

            metricsRecorder.recordBatchWindowExpiry(topic, messageCount);

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
     * Drain and return up to maxItems from the batch queue for a topic.
     * Thread-safe: uses per-topic locking.
     *
     * @param topic The topic to drain
     * @param maxItems Maximum items to drain (0 or negative means drain all)
     * @param recordProcessingTime If true, records current time for fixed window spacing
     * @return Queue containing drained items (up to maxItems)
     */
    public ConcurrentLinkedQueue<Object[]> drainBatch(String topic, int maxItems, boolean recordProcessingTime) {
        ReentrantLock lock = topicLocks.computeIfAbsent(topic, k -> new ReentrantLock());
        lock.lock();
        try {
            ConcurrentLinkedQueue<Object[]> sourceQueue = batchQueues.get(topic);

            if (sourceQueue == null || sourceQueue.isEmpty()) {
                // Nothing to drain
                batchWindowStartTimes.remove(topic);
                cancelWindowExpiryTask(topic);

                AtomicBoolean processing = processingFlags.get(topic);
                if (processing != null) {
                    processing.set(false);
                }
                return null;
            }

            ConcurrentLinkedQueue<Object[]> result;

            if (maxItems <= 0 || sourceQueue.size() <= maxItems) {
                // Drain all - remove the queue entirely
                result = batchQueues.remove(topic);

                AtomicLong counter = batchCounters.get(topic);
                if (counter != null) {
                    counter.set(0);
                }
            } else {
                // Drain only up to maxItems - leave rest for next batch
                result = new ConcurrentLinkedQueue<>();
                for (int i = 0; i < maxItems && !sourceQueue.isEmpty(); i++) {
                    Object[] item = sourceQueue.poll();
                    if (item != null) {
                        result.add(item);
                    }
                }

                // Update counter to reflect remaining items
                AtomicLong counter = batchCounters.get(topic);
                if (counter != null) {
                    counter.set(sourceQueue.size());
                }

                logger.debug("Drained {} items from topic: {}, {} remaining for next batch",
                    result.size(), topic, sourceQueue.size());
            }

            batchWindowStartTimes.remove(topic);
            cancelWindowExpiryTask(topic);

            // Record processing time for fixed window spacing calculation
            if (recordProcessingTime) {
                lastBatchProcessedTimes.put(topic, System.currentTimeMillis());
            }

            // Reset processing flag for next batch
            AtomicBoolean processing = processingFlags.get(topic);
            if (processing != null) {
                processing.set(false);
            }

            return result;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Drain and return the batch queue for a topic, resetting all state atomically.
     * Thread-safe: uses per-topic locking.
     *
     * @param topic The topic to drain
     * @param recordProcessingTime If true, records current time for fixed window spacing
     */
    public ConcurrentLinkedQueue<Object[]> drainBatch(String topic, boolean recordProcessingTime) {
        return drainBatch(topic, 0, recordProcessingTime); // 0 means drain all
    }

    /**
     * Check if there are pending messages and schedule a new window for them.
     * Should be called after batch processing completes.
     */
    public void scheduleNextWindowIfNeeded(String topic, DameroKafkaListener listener) {
        ReentrantLock lock = topicLocks.computeIfAbsent(topic, k -> new ReentrantLock());
        lock.lock();
        try {
            // Check if there are messages that arrived during processing
            if (hasPendingMessages(topic) && windowExpiryTasks.get(topic) == null) {
                batchWindowStartTimes.put(topic, System.currentTimeMillis());
                scheduleWindowExpiry(topic, listener);
                logger.debug("Scheduled new window for {} pending messages on topic: {}",
                    getBatchCount(topic), topic);
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * Drain and return the batch queue for a topic, resetting all state atomically.
     * Thread-safe: uses per-topic locking.
     */
    public ConcurrentLinkedQueue<Object[]> drainBatch(String topic) {
        return drainBatch(topic, true);
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
