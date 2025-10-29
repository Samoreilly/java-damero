package net.damero.CustomObject;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.Setter;
import java.time.LocalDateTime;

@Getter
@Setter
public class EventMetadata {

    private LocalDateTime firstFailureDateTime;
    private LocalDateTime lastFailureDateTime;
    private int attempts;

    @JsonCreator
    public EventMetadata(
            @JsonProperty("firstFailureDateTime") LocalDateTime firstFailureDateTime,
            @JsonProperty("lastFailureDateTime") LocalDateTime lastFailureDateTime,
            @JsonProperty("attempts") int attempts) {
        this.firstFailureDateTime = firstFailureDateTime;
        this.lastFailureDateTime = lastFailureDateTime;
        this.attempts = attempts;
    }
}
