package net.damero.Kafka.DeadLetterQueueAPI;

import net.damero.Kafka.CustomObject.EventWrapper;
import net.damero.Kafka.DeadLetterQueueAPI.ReadFromDLQ.ReadFromDLQConsumer;
import net.damero.Kafka.DeadLetterQueueAPI.ReplayDLQ.ReplayDLQ;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
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

    /**
     * Get the DLQ Dashboard as HTML.
     * Endpoint: GET /dlq/dashboard?topic=test-dlq
     */
    @GetMapping(value = "/dlq/dashboard", produces = MediaType.TEXT_HTML_VALUE)
    @ResponseBody
    public String getDLQDashboard(@RequestParam(required = false, defaultValue = "test-dlq") String topic) {
        DLQResponse response = getDLQ(topic);
        return DLQDashboardRenderer.renderDashboard(response);
    }

    /**
     * Get just the DLQ table partial for HTMX updates.
     * Endpoint: GET /dlq/dashboard/table?topic=test-dlq
     */
    @GetMapping(value = "/dlq/dashboard/table", produces = MediaType.TEXT_HTML_VALUE)
    @ResponseBody
    public String getDLQTablePartial(@RequestParam(required = false, defaultValue = "test-dlq") String topic) {
        DLQResponse response = getDLQ(topic);
        return DLQDashboardRenderer.renderTable(response);
    }

    /**
     * Enhanced replay endpoint that supports HTMX partial updates.
     * If the requester is HTMX, it returns the updated table.
     */
    @PostMapping(value = "/dlq/replay/{topic}", produces = { MediaType.APPLICATION_JSON_VALUE,
            MediaType.TEXT_HTML_VALUE })
    public ResponseEntity<?> replayDLQEndpoint(
            @PathVariable("topic") String topic,
            @RequestParam(required = false, defaultValue = "false") boolean forceReplay,
            @RequestParam(required = false, defaultValue = "false") boolean skipValidation,
            @RequestHeader(value = "HX-Request", required = false) Boolean isHtmx) {

        replayDLQ.replayMessages(topic, forceReplay, skipValidation);

        if (isHtmx != null && isHtmx) {
            // Give it a tiny bit of time for Kafka to process or just return empty table
            // In a real app we might wait, but here we'll just return the current state
            DLQResponse response = getDLQ(topic);
            return ResponseEntity.ok()
                    .header("HX-Trigger", "{\"showMessage\": \"Replay initiated for " + topic + "\"}")
                    .body(DLQDashboardRenderer.renderTable(response));
        }

        return ResponseEntity.ok(new ReplayResponse("Replay initiated for topic: " + topic, topic,
                LocalDateTime.now().format(FORMATTER)));
    }

    /**
     * Overload for direct Java calls (like in tests) to maintain compatibility.
     */
    public ResponseEntity<?> replayDLQEndpoint(String topic, boolean forceReplay, boolean skipValidation) {
        return replayDLQEndpoint(topic, forceReplay, skipValidation, false);
    }

    private DLQSummaryStats calculateStats(List<DLQEventSummary> summaries) {
        if (summaries.isEmpty()) {
            return DLQSummaryStats.builder().totalEvents(0).build();
        }

        int highSeverity = 0;
        int mediumSeverity = 0;
        int lowSeverity = 0;

        for (DLQEventSummary summary : summaries) {
            if (summary == null)
                continue;
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
