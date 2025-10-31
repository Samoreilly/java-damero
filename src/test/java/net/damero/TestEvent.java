package net.damero;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

public class TestEvent {

    private String id;
    private String data;
    private boolean shouldFail;

    // Default constructor for Jackson
    public TestEvent() {
    }

    @JsonCreator
    public TestEvent(
            @JsonProperty("id") String id,
            @JsonProperty("data") String data,
            @JsonProperty("shouldFail") boolean shouldFail) {
        this.id = id;
        this.data = data;
        this.shouldFail = shouldFail;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getData() {
        return data;
    }

    public void setData(String data) {
        this.data = data;
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
                Objects.equals(data, testEvent.data);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, data, shouldFail);
    }

    @Override
    public String toString() {
        return "TestEvent{" +
                "id='" + id + '\'' +
                ", data='" + data + '\'' +
                ", shouldFail=" + shouldFail +
                '}';
    }
}