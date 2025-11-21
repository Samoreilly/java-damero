package net.damero.Kafka.DeadLetterQueueAPI;

import lombok.Builder;
import lombok.Data;

import java.util.Map;

/**
 * Aggregate statistics for all DLQ events
 */
@Data
@Builder
public class DLQSummaryStats {
    
    private int totalEvents;
    private int highSeverityCount;
    private int mediumSeverityCount;
    private int lowSeverityCount;
    
    // Status breakdown
    private Map<String, Integer> statusBreakdown;
    
    // Exception breakdown
    private Map<String, Integer> exceptionTypeBreakdown;
    
    // Topic breakdown
    private Map<String, Integer> topicBreakdown;
    
    // Retry stats
    private double averageAttempts;
    private int maxAttemptsObserved;
    private int minAttemptsObserved;
    
    // Time stats
    private String oldestEvent; // timestamp
    private String newestEvent; // timestamp
}

