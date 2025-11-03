package net.damero;

import net.damero.Kafka.Annotations.CustomKafkaListener;
import net.damero.Kafka.Config.DelayMethod;
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
            delayMethod = DelayMethod.LINEAR
    )
    @KafkaListener(topics = "test-topic", groupId = "test-group", containerFactory = "kafkaListenerContainerFactory")
    public void listen(org.apache.kafka.clients.consumer.ConsumerRecord<String, Object> record, Acknowledgment acknowledgment) {
        if (record == null) {
            return;
        }

        Object payload = record.value();
        TestEvent event = null;
        
        // With header-based approach, payload is always the original event, not EventWrapper
        if (payload instanceof TestEvent te) {
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

        if (event.isShouldFail() && !("retry-success-1".equals(eventId) && attemptCounts.get(eventId) >= 2)) {
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