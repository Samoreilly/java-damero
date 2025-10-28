package net.damero;

import net.damero.CustomObject.EventWrapper;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@Component
public class DLQMessageCollector {

    private final List<EventWrapper<?>> receivedMessages = new ArrayList<>();

    @KafkaListener(
            topics = "test-dlq",
            groupId = "dlq-test-group",
            containerFactory = "dlqKafkaListenerContainerFactory"
    )
    public void listen(EventWrapper<?> message) {
        System.out.println("DLQ received: " + message);
        receivedMessages.add(message);
    }

    public List<EventWrapper<?>> getReceivedMessages() {
        return new ArrayList<>(receivedMessages);
    }

    public void reset() {
        receivedMessages.clear();
    }
}