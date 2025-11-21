package net.damero.Kafka.DeadLetterQueueAPI;

import lombok.Builder;
import lombok.Data;

import java.util.List;

/**
 * Enhanced DLQ API response with summary stats
 */
@Data
@Builder
public class DLQResponse {

    private DLQSummaryStats summary;
    private List<DLQEventSummary> events;

    // Query info
    private String topic;
    private int pageSize;
    private String queriedAt;
}

