package net.damero.CustomObject;

import java.time.LocalDateTime;

public class EventMetadata {

    private LocalDateTime firstFailure;
    private LocalDateTime lastFailure;
    private int retryCount;

    public EventMetadata(LocalDateTime firstFailure, LocalDateTime lastFailure, int retryCount) {
        this.firstFailure = firstFailure;
        this.lastFailure = lastFailure;
        this.retryCount = retryCount;
    }
}
