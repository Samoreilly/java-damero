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

    public EventWrapper(Object event, List<Throwable> throwable){
        this.event = event;
        this.date = LocalDateTime.now();
        exceptions.computeIfAbsent(event, k -> new ArrayList<>()).addAll(throwable);
    }

}
