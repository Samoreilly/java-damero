package net.damero.Kafka.Aspect.Deduplication;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import net.damero.Kafka.Config.DeduplicationProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Manages message deduplication using a bucket-based caching strategy.
 * The idea is to store the caches at different indexes like a bucket sort for faster reads using hashes.
 * Benefits from smaller cache size and cache locality.
 */
@Component
public class DuplicationManager {

    private static final Logger logger = LoggerFactory.getLogger(DuplicationManager.class);
    private static final Object PLACEHOLDER_OBJECT = new Object();

    private final int numBuckets;
    private final List<Cache<String, Object>> cache;

    private final DeduplicationProperties properties;

    // Metrics
    private final AtomicLong duplicateCount = new AtomicLong(0);
    private final AtomicLong totalChecks = new AtomicLong(0);

    public DuplicationManager(DeduplicationProperties properties) {
        this.properties = properties;
        this.numBuckets = properties.getNumBuckets();
        this.cache = new ArrayList<>(numBuckets);

        logger.info("Initializing DuplicationManager with {} buckets, {} {} window, {} max entries per bucket",
                numBuckets,
                properties.getWindowDuration(),
                properties.getWindowUnit(),
                properties.getMaxEntriesPerBucket());

        // initialize the bucket caches
        for (int i = 0; i < numBuckets; i++) {
            cache.add(Caffeine.newBuilder()
                    .expireAfterWrite(properties.getWindowDuration(), properties.getWindowUnit())
                    .maximumSize(properties.getMaxEntriesPerBucket())
                    .build());
        }

        logger.info("DuplicationManager initialized. Total capacity: {} entries", properties.getTotalCapacity());
    }

    /**
     * Check if a message ID is a duplicate.
     * If not seen before, the ID is cached for future checks.
     *
     * @param id the message ID to check
     * @return true if duplicate, false if first time seeing this ID
     */
    public boolean isDuplicate(String id) {
        if (id == null || id.isEmpty()) {
            logger.debug("Cannot deduplicate - eventId is null or empty");
            return false;
        }

        totalChecks.incrementAndGet();

        // get bucket index
        int bucket = Math.floorMod(id.hashCode(), numBuckets);
        Cache<String, Object> bucketCache = cache.get(bucket);

        // check if already seen
        if (bucketCache.getIfPresent(id) != null) {
            duplicateCount.incrementAndGet();
            logger.debug("Duplicate message detected: {}", id);
            return true;
        }

        // mark as seen (value is irrelevant, we just want to see if the event is present)
        bucketCache.put(id, PLACEHOLDER_OBJECT);
        return false;
    }

    /**
     * Get the number of duplicate messages detected.
     */
    public long getDuplicateCount() {
        return duplicateCount.get();
    }

    /**
     * Get the total number of deduplication checks performed.
     */
    public long getTotalChecks() {
        return totalChecks.get();
    }

    /**
     * Get the current cache hit rate (duplicates / total checks).
     */
    public double getHitRate() {
        long total = totalChecks.get();
        return total > 0 ? (double) duplicateCount.get() / total : 0.0;
    }

    /**
     * Get the total number of cached entries across all buckets.
     */
    public long getTotalCacheSize() {
        return cache.stream()
                .mapToLong(Cache::estimatedSize)
                .sum();
    }

    /**
     * Clear all cached entries (for testing or manual intervention).
     */
    public void clearAll() {
        logger.warn("Clearing all deduplication cache entries");
        cache.forEach(Cache::invalidateAll);
        duplicateCount.set(0);
        totalChecks.set(0);
    }

    /**
     * Get deduplication statistics.
     */
    public DeduplicationStats getStats() {
        return new DeduplicationStats(
                duplicateCount.get(),
                totalChecks.get(),
                getTotalCacheSize(),
                properties.getTotalCapacity(),
                getHitRate()
        );
    }

    /**
     * Container for deduplication statistics.
     */
    public static class DeduplicationStats {
        private final long duplicateCount;
        private final long totalChecks;
        private final long currentCacheSize;
        private final long maxCapacity;
        private final double hitRate;

        public DeduplicationStats(long duplicateCount, long totalChecks, long currentCacheSize,
                                  long maxCapacity, double hitRate) {
            this.duplicateCount = duplicateCount;
            this.totalChecks = totalChecks;
            this.currentCacheSize = currentCacheSize;
            this.maxCapacity = maxCapacity;
            this.hitRate = hitRate;
        }

        public long getDuplicateCount() {
            return duplicateCount;
        }

        public long getTotalChecks() {
            return totalChecks;
        }

        public long getCurrentCacheSize() {
            return currentCacheSize;
        }

        public long getMaxCapacity() {
            return maxCapacity;
        }

        public double getHitRate() {
            return hitRate;
        }

        public double getCacheUtilization() {
            return maxCapacity > 0 ? (double) currentCacheSize / maxCapacity : 0.0;
        }

        @Override
        public String toString() {
            return String.format("DeduplicationStats{duplicates=%d, totalChecks=%d, cacheSize=%d/%d (%.2f%%), hitRate=%.4f}",
                    duplicateCount, totalChecks, currentCacheSize, maxCapacity,
                    getCacheUtilization() * 100, hitRate);
        }
    }
}
