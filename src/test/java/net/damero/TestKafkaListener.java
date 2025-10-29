package net.damero;

import net.damero.Annotations.CustomKafkaListener;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

@Component
public class TestKafkaListener {

    private final List<TestEvent> successfulEvents = new CopyOnWriteArrayList<>();
    private final List<TestEvent> failedEvents = new CopyOnWriteArrayList<>();
    private final Map<String, Integer> attemptCounts = new ConcurrentHashMap<>();

    @CustomKafkaListener(
            topic = "test-topic",
            retryable = true,  // ✅ ADD THIS
            retryableTopic = "test-topic-retry",
            dlqTopic = "test-dlq",
            maxAttempts = 3,
            delay = 100,
            delayMethod = net.damero.CustomKafkaSetup.DelayMethod.LINEAR
    )
    @KafkaListener(topics = "test-topic", groupId = "test-group", containerFactory = "kafkaListenerContainerFactory")
    public void listen(TestEvent event, Acknowledgment acknowledgment) {  // ✅ ADD Acknowledgment
        String eventId = event.getId();
        attemptCounts.merge(eventId, 1, Integer::sum);

        System.out.println("Processing event: " + eventId +
                " (attempt " + attemptCounts.get(eventId) + ")");

        if (event.isShouldFail()) {
            failedEvents.add(event);
            throw new RuntimeException("Simulated failure for event: " + eventId);
        }

        successfulEvents.add(event);
    }

    public List<TestEvent> getSuccessfulEvents() {
        return successfulEvents;
    }

    public List<TestEvent> getFailedEvents() {
        return failedEvents;
    }

    public int getAttemptCount(String eventId) {
        return attemptCounts.getOrDefault(eventId, 0);
    }

    public void reset() {
        successfulEvents.clear();
        failedEvents.clear();
        attemptCounts.clear();
    }
}
