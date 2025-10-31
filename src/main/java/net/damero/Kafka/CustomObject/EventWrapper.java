package net.damero.Kafka.CustomObject;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import java.time.LocalDateTime;

public class EventWrapper<T> {

    private LocalDateTime timestamp;
    private EventMetadata metadata;

    // This will store the full class name of the wrapped event
    @JsonTypeInfo(
            use = JsonTypeInfo.Id.CLASS,
            include = JsonTypeInfo.As.PROPERTY,
            property = "@eventClass"
    )
    private T event;

    // Default constructor for Jackson
    public EventWrapper() {
    }

    public EventWrapper(T event, LocalDateTime timestamp, EventMetadata metadata) {
        this.event = event;
        this.timestamp = timestamp;
        this.metadata = metadata;
    }

    // Getters and setters...
    public T getEvent() {
        return event;
    }

    public void setEvent(T event) {
        this.event = event;
    }

    public LocalDateTime getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(LocalDateTime timestamp) {
        this.timestamp = timestamp;
    }

    public EventMetadata getMetadata() {
        return metadata;
    }

    public void setMetadata(EventMetadata metadata) {
        this.metadata = metadata;
    }
}