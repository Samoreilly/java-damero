package net.damero.Kafka.Config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;

/**
 * Configuration properties for Kafka message deduplication.
 * These properties can be configured in application.properties with the prefix "custom.kafka.deduplication".
 */
@Component
@ConfigurationProperties(prefix = "custom.kafka.deduplication")
public class DeduplicationProperties {

    /**
     * Duration for the deduplication window.
     * Default: 10
     */
    private long windowDuration = 10;

    /**
     * Time unit for the deduplication window.
     * Default: HOURS
     */

    private TimeUnit windowUnit = TimeUnit.HOURS;

    /**
     * Number of cache buckets for deduplication.
     * More buckets = better concurrency but more memory overhead.
     * Default: 1000
     */
    private int numBuckets = 1000;

    /**
     * Maximum number of entries per bucket.
     * Total capacity = numBuckets * maxEntriesPerBucket
     * Default: 50000 (50M total capacity with 1000 buckets)
     */
    private int maxEntriesPerBucket = 50000;

    /**
     * Enable or disable deduplication globally.
     * Default: true
     */
    private boolean enabled = true;

    // Getters and Setters

    public long getWindowDuration() {
        return windowDuration;
    }

    public void setWindowDuration(long windowDuration) {
        this.windowDuration = windowDuration;
    }

    public TimeUnit getWindowUnit() {
        return windowUnit;
    }

    public void setWindowUnit(TimeUnit windowUnit) {
        this.windowUnit = windowUnit;
    }

    public int getNumBuckets() {
        return numBuckets;
    }

    public void setNumBuckets(int numBuckets) {
        if (numBuckets <= 0) {
            throw new IllegalArgumentException("numBuckets must be positive");
        }
        this.numBuckets = numBuckets;
    }

    public int getMaxEntriesPerBucket() {
        return maxEntriesPerBucket;
    }

    public void setMaxEntriesPerBucket(int maxEntriesPerBucket) {
        if (maxEntriesPerBucket <= 0) {
            throw new IllegalArgumentException("maxEntriesPerBucket must be positive");
        }
        this.maxEntriesPerBucket = maxEntriesPerBucket;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    /**
     * Get total maximum capacity across all buckets.
     */
    public long getTotalCapacity() {
        return (long) numBuckets * maxEntriesPerBucket;
    }

    @Override
    public String toString() {
        return "DeduplicationProperties{" +
                "windowDuration=" + windowDuration +
                ", windowUnit=" + windowUnit +
                ", numBuckets=" + numBuckets +
                ", maxEntriesPerBucket=" + maxEntriesPerBucket +
                ", totalCapacity=" + getTotalCapacity() +
                ", enabled=" + enabled +
                '}';
    }
}

