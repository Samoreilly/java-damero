package net.damero;

import net.damero.Annotations.CustomKafkaListener;
import net.damero.CustomKafkaSetup.DelayMethod;
import net.damero.CustomObject.EventWrapper;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

@Component
public class DelayTestListener {
    private final AtomicInteger processedCount = new AtomicInteger(0);
    private final Map<String, List<Instant>> attemptTimestamps = new ConcurrentHashMap<>();
    private final List<String> successfulEvents = new CopyOnWriteArrayList<>();

    @CustomKafkaListener(
            topic = "delay-test-topic",
            retryable = true,
            retryableTopic = "delay-test-retry",
            dlqTopic = "delay-test-dlq",
            maxAttempts = 3,
            delay = 500,
            delayMethod = DelayMethod.EXPO
    )
    @KafkaListener(
            topics = "delay-test-topic",
            groupId = "delay-test-group",
            containerFactory = "delayTestContainerFactory"
    )
    public void listen(TestEvent event, Acknowledgment acknowledgment) {
        processedCount.incrementAndGet();

        String eventId = event.getId();
        attemptTimestamps.computeIfAbsent(eventId, k -> new CopyOnWriteArrayList<>())
                .add(Instant.now());

        int attemptNum = attemptTimestamps.get(eventId).size();
        System.out.println("⏱️ Processing: " + eventId +
                " (local attempt " + attemptNum + ") at " + Instant.now());

        if (event.isShouldFail()) {
            throw new RuntimeException("Simulated failure: " + eventId);
        }

        successfulEvents.add(eventId);
    }

    public int getProcessedCount() {
        return processedCount.get();
    }

    public List<Instant> getAttemptTimestamps(String eventId) {
        return attemptTimestamps.getOrDefault(eventId, Collections.emptyList());
    }

    public void reset() {
        processedCount.set(0);
        attemptTimestamps.clear();
        successfulEvents.clear();
    }
}