package net.damero.Kafka.DeadLetterQueueAPI;

import net.damero.Kafka.CustomObject.EventMetadata;
import net.damero.Kafka.CustomObject.EventWrapper;
import net.damero.Kafka.Aspect.Components.Utility.EventUnwrapper;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * Converts EventWrapper to user-friendly DLQEventSummary
 */
public class DLQEventMapper {

    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    public static DLQEventSummary toSummary(EventWrapper<?> wrapper) {
        if (wrapper == null)
            return null;

        EventMetadata metadata = wrapper.getMetadata();

        // If metadata is missing (tests send a bare wrapper), return a minimal summary
        if (metadata == null) {
            String eventId = EventUnwrapper.extractEventId(wrapper.getEvent(), null);
            return DLQEventSummary.builder()
                    .eventId(eventId)
                    .eventType(wrapper.getEvent() != null ? wrapper.getEvent().getClass().getSimpleName() : "Unknown")
                    .status("IN_DLQ")
                    .severity(DLQEventSummary.calculateSeverity(1, 3, null))
                    .originalEvent(wrapper.getEvent())
                    .build();
        }

        LocalDateTime now = LocalDateTime.now();
        LocalDateTime firstFailure = metadata.getFirstFailureDateTime();
        LocalDateTime lastFailure = metadata.getLastFailureDateTime();

        // Calculate durations
        Duration timeInDlq = lastFailure != null ? Duration.between(lastFailure, now) : null;
        Duration failureDuration = (firstFailure != null && lastFailure != null)
                ? Duration.between(firstFailure, lastFailure)
                : null;

        // Build exception info
        DLQEventSummary.ExceptionInfo firstEx = buildExceptionInfo(
                metadata.getFirstFailureException(),
                firstFailure);

        DLQEventSummary.ExceptionInfo lastEx = buildExceptionInfo(
                metadata.getLastFailureException(),
                lastFailure);

        boolean sameExceptionType = firstEx != null && lastEx != null
                && firstEx.getType().equals(lastEx.getType());

        // Determine status
        String status = determineStatus(metadata);

        // Build retry config
        DLQEventSummary.RetryConfig retryConfig = DLQEventSummary.RetryConfig.builder()
                .delayMs(metadata.getDelayMs())
                .delayMethod(metadata.getDelayMethod())
                .maxAttempts(metadata.getMaxAttempts())
                .build();

        // Get event ID if available
        String eventId = EventUnwrapper.extractEventId(wrapper.getEvent(), null);

        return DLQEventSummary.builder()
                .eventId(eventId)
                .eventType(wrapper.getEvent() != null ? wrapper.getEvent().getClass().getSimpleName() : "Unknown")
                .status(status)
                .severity(DLQEventSummary.calculateSeverity(
                        metadata.getAttempts(),
                        metadata.getMaxAttempts(),
                        metadata.getLastFailureException()))
                .firstFailureTime(firstFailure != null ? firstFailure.format(FORMATTER) : null)
                .lastFailureTime(lastFailure != null ? lastFailure.format(FORMATTER) : null)
                .timeInDlq(DLQEventSummary.formatDuration(timeInDlq))
                .failureDuration(DLQEventSummary.formatDuration(failureDuration))
                .totalAttempts(metadata.getAttempts())
                .maxAttemptsConfigured(metadata.getMaxAttempts())
                .maxAttemptsReached(metadata.getAttempts() >= metadata.getMaxAttempts())
                .originalTopic(metadata.getOriginalTopic())
                .dlqTopic(metadata.getDlqTopic())
                .firstException(firstEx)
                .lastException(lastEx)
                .sameExceptionType(sameExceptionType)
                .retryConfig(retryConfig)
                .originalEvent(wrapper.getEvent())
                .build();
    }

    private static DLQEventSummary.ExceptionInfo buildExceptionInfo(Exception ex, LocalDateTime timestamp) {
        if (ex == null)
            return null;

        return DLQEventSummary.ExceptionInfo.builder()
                .type(ex.getClass().getSimpleName())
                .message(ex.getMessage())
                .rootCause(DLQEventSummary.getRootCause(ex))
                .timestamp(timestamp != null ? timestamp.format(FORMATTER) : null)
                .stackTracePreview(DLQEventSummary.getStackTracePreview(ex))
                .build();
    }

    private static String determineStatus(EventMetadata metadata) {
        if (metadata == null)
            return "IN_DLQ";
        if (metadata.getAttempts() >= metadata.getMaxAttempts()) {
            return "FAILED_MAX_RETRIES";
        }
        if (metadata.getAttempts() == 1) {
            return "NON_RETRYABLE_EXCEPTION";
        }
        return "IN_DLQ";
    }
}
