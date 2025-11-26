package net.damero.Kafka.DeadLetterQueueAPI;

import lombok.Builder;
import lombok.Data;

import java.util.List;

/**
 * Enhanced DLQ API response with summary stats and pagination support
 */
@Data
@Builder
public class DLQResponse {

    private DLQSummaryStats summary;
    private List<DLQEventSummary> events;

    // Query info
    private String topic;
    private int pageSize;

    // Pagination metadata
    private int offset;
    private int limit;
    private int totalCached;
    private boolean hasMore;

    private String queriedAt;
}

