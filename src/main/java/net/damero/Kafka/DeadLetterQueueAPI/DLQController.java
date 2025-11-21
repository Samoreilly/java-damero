package net.damero.Kafka.DeadLetterQueueAPI;

import net.damero.Kafka.CustomObject.EventWrapper;
import net.damero.Kafka.DeadLetterQueueAPI.ReadFromDLQ.ReadFromDLQConsumer;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@RestController
public class DLQController {

    private final ReadFromDLQConsumer readFromDLQConsumer;
    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    public DLQController(ReadFromDLQConsumer readFromDLQConsumer) {
        this.readFromDLQConsumer = readFromDLQConsumer;
    }

    /**
     * Get raw DLQ events (original format)
     * Endpoint: GET /dlq/raw?topic=test-dlq
     */
    @GetMapping("/dlq/raw")
    public List<EventWrapper<?>> getDLQRaw(@RequestParam(required = false, defaultValue = "test-dlq") String topic) {
        List<EventWrapper<?>> dlqEvents = readFromDLQConsumer.readFromDLQ(topic);

        if (dlqEvents == null || dlqEvents.isEmpty()) {
            EventWrapper<String> emptyWrapper = new EventWrapper<>();
            emptyWrapper.setEvent("Nothing in Dead Letter Queue");
            return List.of(emptyWrapper);
        }
        return dlqEvents;
    }

    /**
     * Get enhanced DLQ events with better formatting
     * Endpoint: GET /dlq?topic=test-dlq
     */
    @GetMapping("/dlq")
    public DLQResponse getDLQ(@RequestParam(required = false, defaultValue = "test-dlq") String topic) {
        List<EventWrapper<?>> dlqEvents = readFromDLQConsumer.readFromDLQ(topic);

        if (dlqEvents == null || dlqEvents.isEmpty()) {
            return DLQResponse.builder()
                .summary(DLQSummaryStats.builder()
                    .totalEvents(0)
                    .build())
                .events(List.of())
                .topic(topic)
                .pageSize(0)
                .queriedAt(LocalDateTime.now().format(FORMATTER))
                .build();
        }

        // Convert to enhanced summaries
        List<DLQEventSummary> summaries = dlqEvents.stream()
            .map(DLQEventMapper::toSummary)
            .collect(Collectors.toList());

        // Calculate summary stats
        DLQSummaryStats stats = calculateStats(summaries);

        return DLQResponse.builder()
            .summary(stats)
            .events(summaries)
            .topic(topic)
            .pageSize(summaries.size())
            .queriedAt(LocalDateTime.now().format(FORMATTER))
            .build();
    }

    /**
     * Get only the summary statistics
     * Endpoint: GET /dlq/stats?topic=test-dlq
     */
    @GetMapping("/dlq/stats")
    public DLQSummaryStats getDLQStats(@RequestParam(required = false, defaultValue = "test-dlq") String topic) {
        List<EventWrapper<?>> dlqEvents = readFromDLQConsumer.readFromDLQ(topic);

        if (dlqEvents == null || dlqEvents.isEmpty()) {
            return DLQSummaryStats.builder()
                .totalEvents(0)
                .build();
        }

        List<DLQEventSummary> summaries = dlqEvents.stream()
            .map(DLQEventMapper::toSummary)
            .collect(Collectors.toList());

        return calculateStats(summaries);
    }

    private DLQSummaryStats calculateStats(List<DLQEventSummary> summaries) {
        if (summaries.isEmpty()) {
            return DLQSummaryStats.builder().totalEvents(0).build();
        }

        int highSeverity = 0;
        int mediumSeverity = 0;
        int lowSeverity = 0;

        Map<String, Integer> statusBreakdown = new HashMap<>();
        Map<String, Integer> exceptionBreakdown = new HashMap<>();
        Map<String, Integer> topicBreakdown = new HashMap<>();

        int totalAttempts = 0;
        int maxAttempts = Integer.MIN_VALUE;
        int minAttempts = Integer.MAX_VALUE;

        String oldestEvent = null;
        String newestEvent = null;

        for (DLQEventSummary summary : summaries) {
            // Severity
            switch (summary.getSeverity()) {
                case "HIGH" -> highSeverity++;
                case "MEDIUM" -> mediumSeverity++;
                case "LOW" -> lowSeverity++;
            }

            // Status
            statusBreakdown.merge(summary.getStatus(), 1, Integer::sum);

            // Exception type
            if (summary.getLastException() != null) {
                exceptionBreakdown.merge(summary.getLastException().getType(), 1, Integer::sum);
            }

            // Topic
            if (summary.getOriginalTopic() != null) {
                topicBreakdown.merge(summary.getOriginalTopic(), 1, Integer::sum);
            }

            // Attempts
            totalAttempts += summary.getTotalAttempts();
            maxAttempts = Math.max(maxAttempts, summary.getTotalAttempts());
            minAttempts = Math.min(minAttempts, summary.getTotalAttempts());

            // Time tracking
            String firstFailure = summary.getFirstFailureTime();
            if (firstFailure != null) {
                if (oldestEvent == null || firstFailure.compareTo(oldestEvent) < 0) {
                    oldestEvent = firstFailure;
                }
                if (newestEvent == null || firstFailure.compareTo(newestEvent) > 0) {
                    newestEvent = firstFailure;
                }
            }
        }

        return DLQSummaryStats.builder()
            .totalEvents(summaries.size())
            .highSeverityCount(highSeverity)
            .mediumSeverityCount(mediumSeverity)
            .lowSeverityCount(lowSeverity)
            .statusBreakdown(statusBreakdown)
            .exceptionTypeBreakdown(exceptionBreakdown)
            .topicBreakdown(topicBreakdown)
            .averageAttempts(summaries.size() > 0 ? (double) totalAttempts / summaries.size() : 0)
            .maxAttemptsObserved(maxAttempts != Integer.MIN_VALUE ? maxAttempts : 0)
            .minAttemptsObserved(minAttempts != Integer.MAX_VALUE ? minAttempts : 0)
            .oldestEvent(oldestEvent)
            .newestEvent(newestEvent)
            .build();
    }
}
