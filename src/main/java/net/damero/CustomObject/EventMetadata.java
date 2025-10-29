package net.damero.CustomObject;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import net.damero.CustomKafkaSetup.DelayMethod;

import java.time.LocalDateTime;

/**
 * Metadata for tracking retry attempts and configuration
 */
public class EventMetadata {
    private final LocalDateTime firstFailureDateTime;
    private final LocalDateTime lastFailureDateTime;
    private final int attempts;
    private final String originalTopic;

    // Retry configuration
    private final Long delayMs;
    private final DelayMethod delayMethod;
    private final Integer maxAttempts;

    /**
     * Constructor for backward compatibility (without delay config)
     */
    public EventMetadata(LocalDateTime firstFailureDateTime,
                         LocalDateTime lastFailureDateTime,
                         int attempts,
                         String originalTopic) {
        this(firstFailureDateTime, lastFailureDateTime, attempts, originalTopic, null, null, 3);
    }

    /**
     * Full constructor with delay configuration
     */
    @JsonCreator
    public EventMetadata(
            @JsonProperty("firstFailureDateTime") LocalDateTime firstFailureDateTime,
            @JsonProperty("lastFailureDateTime") LocalDateTime lastFailureDateTime,
            @JsonProperty("attempts") int attempts,
            @JsonProperty("originalTopic") String originalTopic,
            @JsonProperty("delayMs") Long delayMs,
            @JsonProperty("delayMethod") DelayMethod delayMethod,
            @JsonProperty("maxAttempts") Integer maxAttempts) {
        this.firstFailureDateTime = firstFailureDateTime;
        this.lastFailureDateTime = lastFailureDateTime;
        this.attempts = attempts;
        this.originalTopic = originalTopic;
        this.delayMs = delayMs;
        this.delayMethod = delayMethod;
        this.maxAttempts = maxAttempts;
    }

    public LocalDateTime getFirstFailureDateTime() {
        return firstFailureDateTime;
    }

    public LocalDateTime getLastFailureDateTime() {
        return lastFailureDateTime;
    }

    public int getAttempts() {
        return attempts;
    }

    public String getOriginalTopic() {
        return originalTopic;
    }

    public Long getDelayMs() {
        return delayMs;
    }

    public DelayMethod getDelayMethod() {
        return delayMethod;
    }
    public Integer getMaxAttempts() {
        return maxAttempts;
    }

    @Override
    public String toString() {
        return "EventMetadata{" +
                "firstFailureDateTime=" + firstFailureDateTime +
                ", lastFailureDateTime=" + lastFailureDateTime +
                ", attempts=" + attempts +
                ", originalTopic='" + originalTopic + '\'' +
                ", delayMs=" + delayMs +
                ", delayMethod=" + delayMethod +
                 ", maxAttempts=" + maxAttempts +
                '}';
    }
}