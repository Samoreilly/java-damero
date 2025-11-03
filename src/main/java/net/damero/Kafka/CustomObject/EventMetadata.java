package net.damero.Kafka.CustomObject;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Builder;
import lombok.Data;
import net.damero.Kafka.Config.DelayMethod;

import java.time.LocalDateTime;

/**
 * Metadata for tracking retry attempts and configuration
 */
@Data
@Builder(toBuilder = true)
public class EventMetadata {


    /*
        WILL DEFAULT TO NULL IF NO VALUE IS GIVEN
     */
    @Builder.Default
    private final LocalDateTime firstFailureDateTime = null;

    @Builder.Default
    private final LocalDateTime lastFailureDateTime = null;

    @Builder.Default
    private final Exception firstFailureException = null;

    @Builder.Default
    private final Exception lastFailureException = null;

    @Builder.Default
    private final int attempts = 0;

    @Builder.Default
    private final String originalTopic = null;

    @Builder.Default
    private final String dlqTopic = null;

    // retry configuration
    @Builder.Default
    private final Long delayMs = null;

    @Builder.Default
    private final DelayMethod delayMethod = DelayMethod.EXPO;

    @Builder.Default
    private final Integer maxAttempts = 3;

    /**
     * Constructor for backward compatibility (without delay config)
     */
    public EventMetadata(LocalDateTime firstFailureDateTime,
                         LocalDateTime lastFailureDateTime,
                         Exception firstFailureException,
                         Exception lastFailureException,
                         int attempts,
                         String originalTopic, String dlqTopic) {
        this(firstFailureDateTime, lastFailureDateTime, firstFailureException, lastFailureException, attempts, originalTopic, dlqTopic, null, null, 3);
    }

    /**
     * Full constructor with delay configuration
     */
    @JsonCreator
    public EventMetadata(
            @JsonProperty("firstFailureDateTime") LocalDateTime firstFailureDateTime,
            @JsonProperty("lastFailureDateTime") LocalDateTime lastFailureDateTime,
            @JsonProperty("firstFailureException") Exception firstFailureException,
            @JsonProperty("lastFailureException") Exception lastFailureException,
            @JsonProperty("attempts") int attempts,
            @JsonProperty("originalTopic") String originalTopic,
            @JsonProperty("dlqTopic") String dlqTopic,
            @JsonProperty("delayMs") Long delayMs,
            @JsonProperty("delayMethod") DelayMethod delayMethod,
            @JsonProperty("maxAttempts") Integer maxAttempts) {
        this.firstFailureDateTime = firstFailureDateTime;
        this.lastFailureDateTime = lastFailureDateTime;
        this.firstFailureException = firstFailureException;
        this.lastFailureException = lastFailureException;
        this.attempts = attempts;
        this.originalTopic = originalTopic;
        this.dlqTopic = dlqTopic;
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
    public String getDlqTopic() {
        return dlqTopic;
    }

    public Exception getLastFailureException() {
        return lastFailureException;
    }
    public Exception getFirstFailureException() {
        return firstFailureException;
    }

    @Override
    public String toString() {
        return "EventMetadata{" +
                "firstFailureDateTime=" + firstFailureDateTime +
                ", lastFailureDateTime=" + lastFailureDateTime +
                ", firstFailureException=" + firstFailureException +
                ", lastFailureException=" + lastFailureException +
                ", attempts=" + attempts +
                ", originalTopic='" + originalTopic + '\'' +
                ", dlqTopic='" + dlqTopic + '\'' +
                ", delayMs=" + delayMs +
                ", delayMethod=" + delayMethod +
                 ", maxAttempts=" + maxAttempts +
                '}';
    }
}