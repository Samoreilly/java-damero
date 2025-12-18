package net.damero.Kafka.DeadLetterQueueAPI;

import net.damero.Kafka.CustomObject.EventWrapper;
import net.damero.Kafka.DeadLetterQueueAPI.ReadFromDLQ.ReadFromDLQConsumer;
import net.damero.Kafka.DeadLetterQueueAPI.ReplayDLQ.ReplayDLQ;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Simple DLQ REST endpoints.
 * Two endpoints: /dlq/raw and /dlq
 */
@RestController
public class DLQController {

    private final ReadFromDLQConsumer readFromDLQConsumer;
    private final ReplayDLQ replayDLQ;
    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    public DLQController(ReadFromDLQConsumer readFromDLQConsumer, ReplayDLQ replayDLQ) {
        this.readFromDLQConsumer = readFromDLQConsumer;
        this.replayDLQ = replayDLQ;
    }

    /**
     * Get raw DLQ events.
     * Endpoint: GET /dlq/raw?topic=test-dlq
     */
    @GetMapping("/dlq/raw")
    public List<EventWrapper<?>> getDLQRaw(@RequestParam(required = false, defaultValue = "test-dlq") String topic) {
        return readFromDLQConsumer.readFromDLQ(topic);
    }

    /**
     * Replay DLQ messages back to the original topic.
     * @param topic the DLQ topic to replay from
     * @param forceReplay if true, replays all messages from beginning; if false (default), only replays unprocessed messages
     * @param skipValidation if true, adds X-Replay-Mode header to skip validation (for testing with invalid data)
     */
    @PostMapping("/dlq/replay/{topic}")
    public ReplayResponse replayDLQEndpoint(
            @PathVariable("topic") String topic,
            @RequestParam(required = false, defaultValue = "false") boolean forceReplay,
            @RequestParam(required = false, defaultValue = "false") boolean skipValidation) {
        replayDLQ.replayMessages(topic, forceReplay, skipValidation);
        return new ReplayResponse("Replay initiated for topic: " + topic, topic, LocalDateTime.now().format(FORMATTER));
    }

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
            .queriedAt(LocalDateTime.now().format(FORMATTER))
            .build();
    }

    private DLQSummaryStats calculateStats(List<DLQEventSummary> summaries) {
        if (summaries.isEmpty()) {
            return DLQSummaryStats.builder().totalEvents(0).build();
        }

        int highSeverity = 0;
        int mediumSeverity = 0;
        int lowSeverity = 0;

        for (DLQEventSummary summary : summaries) {
            if(summary == null) continue;
            switch (summary.getSeverity()) {
                case "HIGH" -> highSeverity++;
                case "MEDIUM" -> mediumSeverity++;
                case "LOW" -> lowSeverity++;
            }
        }

        return DLQSummaryStats.builder()
            .totalEvents(summaries.size())
            .highSeverityCount(highSeverity)
            .mediumSeverityCount(mediumSeverity)
            .lowSeverityCount(lowSeverity)
            .build();
    }
}

