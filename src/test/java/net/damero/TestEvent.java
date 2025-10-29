package net.damero;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

public class TestEvent {
    private String id;
    private String message;
    private boolean shouldFail;

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
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TestEvent testEvent = (TestEvent) o;
        return shouldFail == testEvent.shouldFail &&
                Objects.equals(id, testEvent.id) &&
                Objects.equals(message, testEvent.message);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, message, shouldFail);
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