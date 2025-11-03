package net.damero.Kafka.Aspect.Components;

import net.damero.Kafka.CustomObject.EventWrapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class for unwrapping events from various formats.
 */
public class EventUnwrapper {
    
    private static final Logger logger = LoggerFactory.getLogger(EventUnwrapper.class);

    /**
     * Extracts the actual event payload from various input formats.
     * 
     * @param arg the argument to unwrap (may be ConsumerRecord, EventWrapper, or raw event)
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
     * Extracts event ID using reflection.
     * 
     * @param event the event object
     * @return the event ID as string, or null if extraction fails
     */
    public static String extractEventId(Object event) {
        if (event == null) {
            return null;
        }
        
        try {
            java.lang.reflect.Method getIdMethod = event.getClass().getMethod("getId");
            Object id = getIdMethod.invoke(event);
            return id != null ? id.toString() : null;
        } catch (Exception e) {
            logger.debug("could not extract event id: {}", e.getMessage());
            return null;
        }
    }
}

