package net.damero;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class TestEvent {
    private String id;
    private String message;
    private boolean shouldFail;

    // Default constructor for Jackson
    public TestEvent() {
    }

    @JsonCreator
    public TestEvent(
            @JsonProperty("id") String id,
            @JsonProperty("message") String message,
            @JsonProperty("shouldFail") boolean shouldFail) {
        this.id = id;
        this.message = message;
        this.shouldFail = shouldFail;
    }

    // Getters and setters
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public boolean isShouldFail() {
        return shouldFail;
    }

    public void setShouldFail(boolean shouldFail) {
        this.shouldFail = shouldFail;
    }

    @Override
    public String toString() {
        return "TestEvent{" +
                "id='" + id + '\'' +
                ", message='" + message + '\'' +
                ", shouldFail=" + shouldFail +
                '}';
    }
}