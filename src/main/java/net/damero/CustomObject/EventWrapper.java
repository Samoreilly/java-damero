package net.damero.CustomObject;

import lombok.Getter;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static net.damero.CustomObject.GlobalExceptionMapLogger.exceptions;

@Getter
public class EventWrapper {

    private final Object event;
    private LocalDateTime date;

    public EventWrapper(Object event){
        this.event = event;
        this.date = LocalDateTime.now();
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

}
