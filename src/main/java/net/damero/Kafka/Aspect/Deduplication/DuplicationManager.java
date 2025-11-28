package net.damero.Kafka.Aspect.Deduplication;

import net.damero.Kafka.Config.DeduplicationProperties;
import net.damero.Kafka.Config.PluggableRedisCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Manages message deduplication using TTL-based caching.
 * Uses a simple key-value strategy that works efficiently with both Redis and Caffeine.
 * Redis handles distributed coordination and efficient lookups internally.
 * 
 * Note: The bucket strategy was removed as it only benefits Caffeine's in-memory cache,
 * not Redis which handles key distribution and lookups efficiently internally.
 */
@Component
public class DuplicationManager {

    private static final Logger logger = LoggerFactory.getLogger(DuplicationManager.class);
    private static final String DEDUP_PREFIX = "dedup:";
    private static final Integer SEEN_MARKER = 1;

    private final PluggableRedisCache cache;
    private final DeduplicationProperties properties;
    private final Duration ttl;

    // Metrics
    private final AtomicLong duplicateCount = new AtomicLong(0);
    private final AtomicLong totalChecks = new AtomicLong(0);

    public DuplicationManager(DeduplicationProperties properties, PluggableRedisCache cache) {
        this.cache = cache;
        this.properties = properties;
        
        // Calculate TTL from window duration and unit
        this.ttl = Duration.ofMillis(properties.getWindowUnit().toMillis(properties.getWindowDuration()));

        logger.info("Initializing DuplicationManager with {} {} window (TTL: {})",
                properties.getWindowDuration(),
                properties.getWindowUnit(),
                ttl);

        logger.info("DuplicationManager initialized. Max capacity: {} entries", properties.getTotalCapacity());
    }

    /**
     * Generate a cache key for the given message ID.
     * Simple prefix-based key for efficient Redis lookups.
     */
    private String getKey(String id) {
        return DEDUP_PREFIX + id;
    }

    /**
     * Check if a message ID has been seen before (is a duplicate).
     * This method does NOT mark the ID as seen - use markAsSeen() after successful processing.
     *
     * @param id the message ID to check
     * @return true if duplicate (already seen), false if first time
     */
    public boolean isDuplicate(String id) {
        if (id == null || id.isEmpty()) {
            logger.debug("Cannot deduplicate - eventId is null or empty");
            return false;
        }

        totalChecks.incrementAndGet();
        String key = getKey(id);
        boolean isDupe = cache.contains(key);
        
        if (isDupe) {
            duplicateCount.incrementAndGet();
            logger.debug("Duplicate detected for message ID: {}", id);
        }
        
        return isDupe;
    }

    /**
     * Mark a message ID as seen to prevent duplicate processing.
     * The entry will automatically expire after the configured window duration.
     *
     * @param id the message ID to mark as seen
     */
    public void markAsSeen(String id) {
        if (id == null || id.isEmpty()) {
            logger.debug("Cannot mark as seen - eventId is null or empty");
            return;
        }
        
        String key = getKey(id);
        cache.put(key, SEEN_MARKER, ttl);
        logger.debug("Marked message ID as seen with TTL {}: {}", ttl, id);
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
     * Note: This is an estimate based on metrics since we can't directly count Redis keys efficiently.
     */
    public long getTotalCacheSize() {
        // Return approximate size based on successful checks
        // In a Redis-backed implementation, counting all keys would be expensive
        // So we return the duplicate count as an estimate
        return duplicateCount.get();
    }

    /**
     * Clear all cached entries (for testing or manual intervention).
     * Note: When using Redis, this clears all dedup entries with the DEDUP_PREFIX.
     * When using Caffeine, this is not fully supported in the current implementation.
     */
    public void clearAll() {
        logger.warn("Clearing deduplication metrics (note: cache entries may persist)");
        // Reset metrics
        duplicateCount.set(0);
        totalChecks.set(0);
        
        // Note: Clearing all keys from cache is not implemented to avoid
        // expensive operations in Redis. Consider implementing TTL-based expiry instead.
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

