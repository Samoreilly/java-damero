
package net.damero;

import net.damero.Annotations.CustomKafkaListener;
import net.damero.CustomKafkaSetup.DelayMethod;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

@Component
public class TestKafkaListener {

    private static final Logger log = LoggerFactory.getLogger(TestKafkaListener.class);

    private final List<TestEvent> successfulEvents = new ArrayList<>();
    private final List<TestEvent> failedEvents = new ArrayList<>();
    private final ConcurrentHashMap<String, AtomicInteger> attemptCounts = new ConcurrentHashMap<>();

    @CustomKafkaListener(
            topic = "test-topic",
            dlqTopic = "test-dlq",
            maxAttempts = 3,
            delay = 100,
            delayMethod = DelayMethod.LINEAR
    )
    @KafkaListener(topics = "test-topic", groupId = "test-group")
    public void handleTestEvent(TestEvent event) {
        String eventId = event.getId();

        //tracks attempts
        AtomicInteger count = attemptCounts.computeIfAbsent(eventId, k -> new AtomicInteger(0));
        int currentAttempt = count.incrementAndGet();

        log.info("Processing event: {} (attempt {})", eventId, currentAttempt);

        if (event.isShouldFail()) {
            failedEvents.add(event);
            log.error("Event {} configured to fail", eventId);
            throw new RuntimeException("Simulated failure for event: " + eventId);
        }

        successfulEvents.add(event);
        log.info("Successfully processed event: {}", eventId);
    }

    // Helper methods for testing
    public List<TestEvent> getSuccessfulEvents() {
        return new ArrayList<>(successfulEvents);
    }

    public List<TestEvent> getFailedEvents() {
        return new ArrayList<>(failedEvents);
    }

    public int getAttemptCount(String eventId) {
        AtomicInteger count = attemptCounts.get(eventId);
        return count != null ? count.get() : 0;
    }

    public void reset() {
        successfulEvents.clear();
        failedEvents.clear();
        attemptCounts.clear();
    }
}