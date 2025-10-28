package net.damero.CustomObject;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.LocalDateTime;

@Getter
@Setter
@NoArgsConstructor
public class EventWrapper {

    private Object event;
    private LocalDateTime date;
    private EventMetadata metadata;

    @JsonCreator
    public EventWrapper(@JsonProperty("event") Object event,
                        @JsonProperty("date") LocalDateTime date,
                        @JsonProperty("metadata") EventMetadata metadata) {
        this.event = event;
        this.date = date;
        this.metadata = metadata;
    }

    public EventWrapper(Object event, EventMetadata metadata){
        this.event = event;
        this.date = LocalDateTime.now();
        this.metadata = metadata;
    }

    //override hashcode to ignore date when equals is called
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if(!(o instanceof EventWrapper)) return false;
        EventWrapper that = (EventWrapper) o;
        return event.equals(that.event);
    }
    
    @Override
    public int hashCode() {
        return event.hashCode();
    }
    
    @Override
    public String toString() {
        return "EventWrapper{" +
                "event=" + event +
                ", date=" + date +
                ", metadata=" + metadata +
                '}';
    }

}
