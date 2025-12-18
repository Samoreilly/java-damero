package net.damero.Kafka.Aspect.Components.Utility;

import net.damero.Kafka.CustomObject.EventWrapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Utility class for unwrapping events from various formats.
 */
public class EventUnwrapper {

    private static final Logger logger = LoggerFactory.getLogger(EventUnwrapper.class);

    // Maximum length for using raw payload as an ID. Longer payloads will be
    // hashed.
    private static final int MAX_ID_LENGTH = 256;

    /**
     * Extracts the actual event payload from various input formats.
     * 
     * @param arg the argument to unwrap (may be ConsumerRecord, EventWrapper, or
     *            raw event)
     * @return the unwrapped event, or null if not found
     */
    public static Object unwrapEvent(Object arg) {
        if (arg == null) {
            return null;
        }

        // Handle ConsumerRecord
        if (arg instanceof ConsumerRecord<?, ?> record) {
            return record.value();
        }

        // Handle EventWrapper
        if (arg instanceof EventWrapper<?> ew) {
            Object inner = ew.getEvent();
            // Handle nested ConsumerRecord in EventWrapper
            if (inner instanceof ConsumerRecord<?, ?> innerRec) {
                return innerRec.value();
            }
            return inner;
        }

        // Already unwrapped event
        return arg;
    }

    /**
     * Extracts the original event from an EventWrapper or returns the event itself.
     * 
     * @param event the event (may be wrapped or unwrapped)
     * @return the original unwrapped event
     */

    public static Object extractOriginalEvent(Object event) {
        if (event instanceof EventWrapper<?> we) {
            Object inner = we.getEvent();
            if (inner instanceof ConsumerRecord<?, ?> rec) {
                return rec.value();
            }
            return inner;
        }

        if (event instanceof ConsumerRecord<?, ?> rec) {
            return rec.value();
        }

        return event;
    }

    /**
     * Extracts event ID using reflection and canonicalization.
     *
     * This method:
     * - treats plain String payloads specially (short strings returned verbatim,
     * long strings hashed)
     * - tries getId()/id reflection for POJOs
     * - falls back to toString() (with length/hash rules)
     *
     * @param event the event object
     * @return the event ID as string, or null if extraction fails
     */
    /**
     * Extracts event ID using reflection, canonicalization, and Kafka coordinates
     * as fallback.
     *
     * @param event  the event object
     * @param record optional ConsumerRecord to provide fallback ID
     *               (topic:partition:offset)
     * @return the event ID as string, or null if extraction fails
     */
    public static String extractEventId(Object event, ConsumerRecord<?, ?> record) {
        if (event == null) {
            return null;
        }

        // 1. Try to find a logical ID in the payload if it's not a core JDK type
        boolean isJDKType = event instanceof CharSequence || event instanceof Number ||
                event instanceof Boolean || event instanceof Character ||
                event.getClass().getName().startsWith("java.");

        if (!isJDKType) {
            try {
                java.lang.reflect.Method getIdMethod = event.getClass().getMethod("getId");
                Object id = getIdMethod.invoke(event);
                String cid = canonicalizeIfNeeded(id);
                if (cid != null)
                    return cid;
            } catch (Exception ignored) {
            }

            try {
                java.lang.reflect.Field idField = event.getClass().getField("id");
                Object id = idField.get(event);
                String cid = canonicalizeIfNeeded(id);
                if (cid != null)
                    return cid;
            } catch (Exception ignored) {
            }
        }

        // 2. Fallback to Kafka coordinates if ConsumerRecord is available
        // This is the most reliable way to uniquely identify a message even if the
        // payload is identical
        if (record != null) {
            return String.format("%s:%d:%d", record.topic(), record.partition(), record.offset());
        }

        // 3. Last resort: hash of toString()
        return canonicalizeStringId(event.toString());
    }

    /**
     * Deprecated: use extractEventId(Object, ConsumerRecord) for better uniqueness.
     */
    @Deprecated
    public static String extractEventId(Object event) {
        return extractEventId(event, null);
    }

    private static String canonicalizeIfNeeded(Object id) {
        if (id == null)
            return null;
        if (id instanceof String s)
            return canonicalizeStringId(s);
        return canonicalizeStringId(id.toString());
    }

    private static String canonicalizeStringId(String s) {
        if (s == null)
            return null;
        if (s.length() <= MAX_ID_LENGTH)
            return s;
        // For long payloads, return a stable hashed representation prefixed for clarity
        try {
            String hash = sha256Hex(s);
            return "h:" + hash;
        } catch (RuntimeException rte) {
            // If hashing fails for any reason, fall back to a truncated preview
            return "t:" + s.substring(0, Math.min(64, s.length()));
        }
    }

    private static String sha256Hex(String input) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] digest = md.digest(input.getBytes(StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder(digest.length * 2);
            for (byte b : digest) {
                sb.append(String.format("%02x", b));
            }
            return sb.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("SHA-256 not available", e);
        }
    }
}
