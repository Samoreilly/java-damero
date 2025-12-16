package net.damero.Kafka.Aspect.Components.Utility;

import net.damero.Kafka.CustomObject.EventMetadata;
import net.damero.Kafka.Config.DelayMethod;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * Utility class for reading and writing retry metadata in Kafka headers.
 * This allows users to receive their original event object with metadata stored in headers.
 */
public class HeaderUtils {
    
    private static final Logger logger = LoggerFactory.getLogger(HeaderUtils.class);
    
    // header key constants
    private static final String HEADER_ATTEMPTS = "damero-retry-attempts";
    private static final String HEADER_FIRST_FAILURE_DATETIME = "damero-first-failure-datetime";
    private static final String HEADER_LAST_FAILURE_DATETIME = "damero-last-failure-datetime";
    private static final String HEADER_FIRST_FAILURE_EXCEPTION = "damero-first-failure-exception";
    private static final String HEADER_LAST_FAILURE_EXCEPTION = "damero-last-failure-exception";
    private static final String HEADER_ORIGINAL_TOPIC = "damero-original-topic";
    private static final String HEADER_DLQ_TOPIC = "damero-dlq-topic";
    private static final String HEADER_DELAY_MS = "damero-delay-ms";
    private static final String HEADER_DELAY_METHOD = "damero-delay-method";
    private static final String HEADER_MAX_ATTEMPTS = "damero-max-attempts";
    // Spring Kafka type header used by JsonDeserializer/JsonSerializer
    private static final String HEADER_TYPE_ID = "__TypeId__";

    private static final DateTimeFormatter DATETIME_FORMATTER = DateTimeFormatter.ISO_LOCAL_DATE_TIME;
    
    /**
     * Extracts EventMetadata from Kafka headers if present.
     * Returns null if headers are not present (first attempt).
     * 
     * @param headers the Kafka headers
     * @return EventMetadata if headers exist, null otherwise
     */
    public static EventMetadata extractMetadataFromHeaders(Headers headers) {
        if (headers == null || !hasRetryHeaders(headers)) {
            return null;
        }
        
        try {
            LocalDateTime firstFailureDateTime = parseDateTime(
                getHeaderValue(headers, HEADER_FIRST_FAILURE_DATETIME)
            );
            LocalDateTime lastFailureDateTime = parseDateTime(
                getHeaderValue(headers, HEADER_LAST_FAILURE_DATETIME)
            );
            
            Exception firstFailureException = parseException(
                getHeaderValue(headers, HEADER_FIRST_FAILURE_EXCEPTION)
            );
            Exception lastFailureException = parseException(
                getHeaderValue(headers, HEADER_LAST_FAILURE_EXCEPTION)
            );
            
            int attempts = parseInt(getHeaderValue(headers, HEADER_ATTEMPTS), 0);
            String originalTopic = getHeaderValue(headers, HEADER_ORIGINAL_TOPIC);
            String dlqTopic = getHeaderValue(headers, HEADER_DLQ_TOPIC);
            Long delayMs = parseLong(getHeaderValue(headers, HEADER_DELAY_MS));
            DelayMethod delayMethod = parseDelayMethod(getHeaderValue(headers, HEADER_DELAY_METHOD));
            Integer maxAttempts = parseInt(getHeaderValue(headers, HEADER_MAX_ATTEMPTS), 3);
            
            return new EventMetadata(
                firstFailureDateTime,
                lastFailureDateTime,
                firstFailureException,
                lastFailureException,
                attempts,
                originalTopic,
                dlqTopic,
                delayMs,
                delayMethod,
                maxAttempts
            );
        } catch (Exception e) {
            logger.warn("failed to extract metadata from headers: {}", e.getMessage());
            return null;
        }
    }
    
    /**
     * Ensures the Spring Kafka type header (__TypeId__) is present on the provided headers.
     * Priority: explicitType (from annotation) -> originalEvent.getClass() -> no-op.
     * If a type header already exists, this method does not override it.
     */
    public static void ensureTypeHeader(Headers headers, Class<?> explicitType, Object originalEvent) {
        if (headers == null) return;
        Header existing = headers.lastHeader(HEADER_TYPE_ID);
        if (existing != null) return; // already present

        String typeName = null;
        if (explicitType != null && explicitType != Void.class) {
            typeName = explicitType.getName();
        } else if (originalEvent != null) {
            typeName = originalEvent.getClass().getName();
        }

        if (typeName != null) {
            headers.add(HEADER_TYPE_ID, typeName.getBytes(StandardCharsets.UTF_8));
            logger.debug("added type header {} = {}", HEADER_TYPE_ID, typeName);
        }
    }

    /**
     * Builds Kafka headers from EventMetadata for retrying a message.
     * 
     * @param existingMetadata the existing metadata extracted from headers (may be null)
     * @param customMetadata new metadata to merge with existing (for updates)
     * @param newAttempts the attempt count to encode
     * @param newException the most recent exception
     * @return RecordHeaders with all metadata encoded
     */
    public static RecordHeaders buildHeadersFromMetadata(EventMetadata existingMetadata, 
                                                         EventMetadata customMetadata,
                                                         int newAttempts,
                                                         Exception newException) {
        RecordHeaders headers = new RecordHeaders();
        
        // Use existing metadata as base, or custom metadata, or defaults
        LocalDateTime firstFailureDateTime = existingMetadata != null && existingMetadata.getFirstFailureDateTime() != null
            ? existingMetadata.getFirstFailureDateTime()
            : (customMetadata != null && customMetadata.getFirstFailureDateTime() != null
                ? customMetadata.getFirstFailureDateTime()
                : LocalDateTime.now());
        
        LocalDateTime lastFailureDateTime = LocalDateTime.now();
        
        Exception firstFailureException = existingMetadata != null && existingMetadata.getFirstFailureException() != null
            ? existingMetadata.getFirstFailureException()
            : (customMetadata != null && customMetadata.getFirstFailureException() != null
                ? customMetadata.getFirstFailureException()
                : newException);
        
        Exception lastFailureException = newException;
        
        String originalTopic = existingMetadata != null && existingMetadata.getOriginalTopic() != null
            ? existingMetadata.getOriginalTopic()
            : (customMetadata != null ? customMetadata.getOriginalTopic() : null);
        
        String dlqTopic = existingMetadata != null && existingMetadata.getDlqTopic() != null
            ? existingMetadata.getDlqTopic()
            : (customMetadata != null ? customMetadata.getDlqTopic() : null);
        
        Long delayMs = existingMetadata != null && existingMetadata.getDelayMs() != null
            ? existingMetadata.getDelayMs()
            : (customMetadata != null && customMetadata.getDelayMs() != null
                ? customMetadata.getDelayMs()
                : null);
        
        DelayMethod delayMethod = existingMetadata != null && existingMetadata.getDelayMethod() != null
            ? existingMetadata.getDelayMethod()
            : (customMetadata != null && customMetadata.getDelayMethod() != null
                ? customMetadata.getDelayMethod()
                : DelayMethod.EXPO);
        
        Integer maxAttempts = existingMetadata != null && existingMetadata.getMaxAttempts() != null
            ? existingMetadata.getMaxAttempts()
            : (customMetadata != null && customMetadata.getMaxAttempts() != null
                ? customMetadata.getMaxAttempts()
                : 3);
        
        // Add headers
        headers.add(HEADER_ATTEMPTS, String.valueOf(newAttempts).getBytes(StandardCharsets.UTF_8));
        headers.add(HEADER_FIRST_FAILURE_DATETIME, 
            firstFailureDateTime.format(DATETIME_FORMATTER).getBytes(StandardCharsets.UTF_8));
        headers.add(HEADER_LAST_FAILURE_DATETIME, 
            lastFailureDateTime.format(DATETIME_FORMATTER).getBytes(StandardCharsets.UTF_8));
        
        if (firstFailureException != null) {
            headers.add(HEADER_FIRST_FAILURE_EXCEPTION, 
                serializeException(firstFailureException).getBytes(StandardCharsets.UTF_8));
        }
        
        if (lastFailureException != null) {
            headers.add(HEADER_LAST_FAILURE_EXCEPTION, 
                serializeException(lastFailureException).getBytes(StandardCharsets.UTF_8));
        }
        
        if (originalTopic != null) {
            headers.add(HEADER_ORIGINAL_TOPIC, originalTopic.getBytes(StandardCharsets.UTF_8));
        }
        
        if (dlqTopic != null) {
            headers.add(HEADER_DLQ_TOPIC, dlqTopic.getBytes(StandardCharsets.UTF_8));
        }
        
        if (delayMs != null) {
            headers.add(HEADER_DELAY_MS, String.valueOf(delayMs).getBytes(StandardCharsets.UTF_8));
        }
        
        headers.add(HEADER_DELAY_METHOD, delayMethod.name().getBytes(StandardCharsets.UTF_8));
        headers.add(HEADER_MAX_ATTEMPTS, String.valueOf(maxAttempts).getBytes(StandardCharsets.UTF_8));
        
        return headers;
    }
    
    /**
     * Builds headers for initial retry configuration (before first failure).
     */
    public static RecordHeaders buildInitialHeaders(EventMetadata config) {
        RecordHeaders headers = new RecordHeaders();
        
        headers.add(HEADER_ATTEMPTS, "0".getBytes(StandardCharsets.UTF_8));
        headers.add(HEADER_ORIGINAL_TOPIC, 
            (config != null && config.getOriginalTopic() != null ? config.getOriginalTopic() : "")
                .getBytes(StandardCharsets.UTF_8));
        headers.add(HEADER_DLQ_TOPIC, 
            (config != null && config.getDlqTopic() != null ? config.getDlqTopic() : "")
                .getBytes(StandardCharsets.UTF_8));
        headers.add(HEADER_DELAY_MS, 
            String.valueOf(config != null && config.getDelayMs() != null ? config.getDelayMs() : 0)
                .getBytes(StandardCharsets.UTF_8));
        headers.add(HEADER_DELAY_METHOD, 
            (config != null && config.getDelayMethod() != null ? config.getDelayMethod() : DelayMethod.EXPO).name()
                .getBytes(StandardCharsets.UTF_8));
        headers.add(HEADER_MAX_ATTEMPTS, 
            String.valueOf(config != null && config.getMaxAttempts() != null ? config.getMaxAttempts() : 3)
                .getBytes(StandardCharsets.UTF_8));
        
        return headers;
    }
    
    private static boolean hasRetryHeaders(Headers headers) {
        return headers.lastHeader(HEADER_ATTEMPTS) != null;
    }
    
    private static String getHeaderValue(Headers headers, String key) {
        Header header = headers.lastHeader(key);
        return header != null ? new String(header.value(), StandardCharsets.UTF_8) : null;
    }
    
    private static LocalDateTime parseDateTime(String value) {
        if (value == null || value.isEmpty()) {
            return null;
        }
        try {
            return LocalDateTime.parse(value, DATETIME_FORMATTER);
        } catch (Exception e) {
            logger.debug("failed to parse datetime from header: {}", value);
            return null;
        }
    }
    
    private static Exception parseException(String value) {
        if (value == null || value.isEmpty()) {
            return null;
        }
        try {
            // Simple exception serialization: class name + message
            String[] parts = value.split(":", 2);
            if (parts.length == 2) {
                Class<?> clazz = Class.forName(parts[0]);
                return (Exception) clazz.getConstructor(String.class).newInstance(parts[1]);
            }
        } catch (Exception e) {
            logger.debug("failed to parse exception from header: {}", value);
        }
        return new RuntimeException(value); // Fallback
    }
    
    private static String serializeException(Exception e) {
        if (e == null) {
            return null;
        }
        return e.getClass().getName() + ":" + e.getMessage();
    }
    
    private static int parseInt(String value, int defaultValue) {
        if (value == null || value.isEmpty()) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }
    
    private static Long parseLong(String value) {
        if (value == null || value.isEmpty()) {
            return null;
        }
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException e) {
            return null;
        }
    }
    
    private static DelayMethod parseDelayMethod(String value) {
        if (value == null || value.isEmpty()) {
            return DelayMethod.EXPO;
        }
        try {
            return DelayMethod.valueOf(value);
        } catch (IllegalArgumentException e) {
            return DelayMethod.EXPO;
        }
    }
}
