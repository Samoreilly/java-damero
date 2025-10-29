package net.damero;

import net.damero.CustomObject.EventWrapper;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

@Component
public class DelayDLQCollector {
    private final List<EventWrapper<?>> receivedMessages = new CopyOnWriteArrayList<>();

    @KafkaListener(
            topics = "delay-test-dlq",
            groupId = "delay-dlq-group",
            containerFactory = "dlqKafkaListenerContainerFactory"
    )
    public void collect(EventWrapper<?> message) {
        System.out.println("📨 DLQ received: Event ID from metadata");
        receivedMessages.add(message);
    }

    public List<EventWrapper<?>> getReceivedMessages() {
        return new ArrayList<>(receivedMessages);
    }

    public void reset() {
        receivedMessages.clear();
    }
}