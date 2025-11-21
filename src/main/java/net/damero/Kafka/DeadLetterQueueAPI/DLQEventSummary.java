package net.damero.Kafka.DeadLetterQueueAPI;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Builder;
import lombok.Data;
import net.damero.Kafka.Config.DelayMethod;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * Enhanced DLQ event summary with better readability and additional insights
 */
@Data
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
public class DLQEventSummary {
    
    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    
    // Basic Info
    private String eventId;
    private String eventType;
    private String status; // "FAILED_MAX_RETRIES", "NON_RETRYABLE", "CIRCUIT_BREAKER_OPEN"
    private String severity; // "HIGH", "MEDIUM", "LOW"
    
    // Timing Info - Human Readable
    private String firstFailureTime;
    private String lastFailureTime;
    private String timeInDlq; // "2 hours 15 minutes"
    private String failureDuration; // Time between first and last failure
    
    // Attempt Info
    private int totalAttempts;
    private int maxAttemptsConfigured;
    private boolean maxAttemptsReached;
    
    // Topic Info
    private String originalTopic;
    private String dlqTopic;
    
    // Exception Info - Clean and readable
    private ExceptionInfo firstException;
    private ExceptionInfo lastException;
    private boolean sameExceptionType;
    
    // Retry Config
    private RetryConfig retryConfig;
    
    // Original Event
    private Object originalEvent;
    
    /**
     * Simplified exception information
     */
    @Data
    @Builder
    public static class ExceptionInfo {
        private String type;
        private String message;
        private String rootCause;
        private String timestamp;
        
        // Top 3 stack trace lines for debugging
        private String[] stackTracePreview;
    }
    
    /**
     * Retry configuration used
     */
    @Data
    @Builder
    public static class RetryConfig {
        private Long delayMs;
        private DelayMethod delayMethod;
        private Integer maxAttempts;
        private String[] nonRetryableExceptions;
    }
    
    /**
     * Calculate severity based on attempts and exception type
     */
    public static String calculateSeverity(int attempts, int maxAttempts, Exception exception) {
        if (attempts >= maxAttempts) return "HIGH";
        if (exception instanceof IllegalArgumentException || 
            exception instanceof NullPointerException) return "MEDIUM";
        return "LOW";
    }
    
    /**
     * Format duration in human-readable format
     */
    public static String formatDuration(Duration duration) {
        if (duration == null) return null;
        
        long hours = duration.toHours();
        long minutes = duration.toMinutes() % 60;
        long seconds = duration.getSeconds() % 60;
        
        if (hours > 0) {
            return String.format("%d hours %d minutes", hours, minutes);
        } else if (minutes > 0) {
            return String.format("%d minutes %d seconds", minutes, seconds);
        } else {
            return String.format("%d seconds", seconds);
        }
    }
    
    /**
     * Get stack trace preview (top 3 lines)
     */
    public static String[] getStackTracePreview(Exception ex) {
        if (ex == null || ex.getStackTrace() == null) return null;
        
        StackTraceElement[] stackTrace = ex.getStackTrace();
        int limit = Math.min(3, stackTrace.length);
        String[] preview = new String[limit];
        
        for (int i = 0; i < limit; i++) {
            preview[i] = stackTrace[i].toString();
        }
        
        return preview;
    }
    
    /**
     * Get root cause message
     */
    public static String getRootCause(Exception ex) {
        if (ex == null) return null;
        
        Throwable cause = ex;
        while (cause.getCause() != null && cause.getCause() != cause) {
            cause = cause.getCause();
        }
        
        return cause != ex ? cause.getClass().getSimpleName() + ": " + cause.getMessage() : null;
    }
}

