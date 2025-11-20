package net.damero;

import net.damero.Kafka.CustomObject.EventWrapper;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

@Component
public class DLQMessageCollector {
    private EventWrapper<?> lastMessage;
    private final Map<String, CopyOnWriteArrayList<EventWrapper<?>>> messagesByTopic = new ConcurrentHashMap<>();
    private final Map<String, CountDownLatch> latches = new ConcurrentHashMap<>();

    public void collect(EventWrapper<?> message) {
        lastMessage = message;
    }

    public EventWrapper<?> getLastMessage() { return lastMessage; }

    public void reset() { lastMessage = null; }

    public void clearMessages() {
        messagesByTopic.clear();
        latches.clear();
    }

    public int getMessageCount(String topic) {
        return messagesByTopic.getOrDefault(topic, new CopyOnWriteArrayList<>()).size();
    }

    public boolean awaitMessages(String topic, int count, long timeout, TimeUnit unit) throws InterruptedException {
        long deadline = System.currentTimeMillis() + unit.toMillis(timeout);
        while (System.currentTimeMillis() < deadline) {
            if (getMessageCount(topic) >= count) {
                return true;
            }
            Thread.sleep(100);
        }
        return getMessageCount(topic) >= count;
    }

    private void addMessage(String topic, EventWrapper<?> message) {
        messagesByTopic.computeIfAbsent(topic, k -> new CopyOnWriteArrayList<>()).add(message);
    }

    @KafkaListener(topics = "test-dlq", groupId = "test-dlq-group", containerFactory = "dlqKafkaListenerContainerFactory")
    public void listenDlq(EventWrapper<?> wrapper) {
        collect(wrapper);
    }

    @KafkaListener(topics = "validation-dlq", groupId = "validation-dlq-group", containerFactory = "kafkaListenerContainerFactory")
    public void listenValidationDlq(EventWrapper<?> wrapper) {
        addMessage("validation-dlq", wrapper);
    }

    @KafkaListener(topics = "timeout-dlq", groupId = "timeout-dlq-group", containerFactory = "kafkaListenerContainerFactory")
    public void listenTimeoutDlq(EventWrapper<?> wrapper) {
        addMessage("timeout-dlq", wrapper);
    }

    @KafkaListener(topics = "default-dlq", groupId = "default-dlq-group", containerFactory = "kafkaListenerContainerFactory")
    public void listenDefaultDlq(EventWrapper<?> wrapper) {
        addMessage("default-dlq", wrapper);
    }

}
