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
            retryableTopic = "test-topic-retry",
            dlqTopic = "test-dlq",
            maxAttempts = 3,
            delay = 100,
            delayMethod = net.damero.CustomKafkaSetup.DelayMethod.LINEAR
    )
    @KafkaListener(topics = "test-topic", groupId = "test-group", containerFactory = "kafkaListenerContainerFactory")
    public void listen(org.apache.kafka.clients.consumer.ConsumerRecord<String, Object> record, Acknowledgment acknowledgment) {
        if (record == null) {
            return;
        }

        Object payload = record.value();
        TestEvent event = null;
        if (payload instanceof net.damero.CustomObject.EventWrapper<?> wrapper) {
            Object inner = wrapper.getEvent();
            if (inner instanceof TestEvent) {
                event = (TestEvent) inner;
            }
        } else if (payload instanceof TestEvent te) {
            event = te;
        } else if (payload instanceof java.util.Map<?, ?> map) {
                Object id = map.get("id");
                Object data = map.get("data");
                Object shouldFail = map.get("shouldFail");
                if (id instanceof String) {
                    event = new TestEvent((String) id, data != null ? data.toString() : null, Boolean.TRUE.equals(shouldFail));
                }
        }
        if (event == null) {
            return;
        }
        String eventId = event.getId();
        attemptCounts.merge(eventId, 1, Integer::sum);

        System.out.println("🔵 Processing event: " + eventId +
                " (attempt " + attemptCounts.get(eventId) + ")");

        if (event.isShouldFail() && !("retry-success-1".equals(eventId) && attemptCounts.get(eventId) >= 2)) {
            failedEvents.add(event);
            System.out.println("❌ Event " + eventId + " is configured to fail");
            throw new RuntimeException("Simulated failure for event: " + eventId);
        }

        successfulEvents.add(event);
        System.out.println("✅ Event " + eventId + " processed successfully");
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
        System.out.println("🔄 TestKafkaListener reset");
    }
}