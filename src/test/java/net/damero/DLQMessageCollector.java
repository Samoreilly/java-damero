package net.damero;

import net.damero.Kafka.CustomObject.EventWrapper;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@Component
public class DLQMessageCollector {
    private EventWrapper<?> lastMessage;

    public void collect(EventWrapper<?> message) {
        lastMessage = message;
    }

    public EventWrapper<?> getLastMessage() { return lastMessage; }

    public void reset() { lastMessage = null; }

    @KafkaListener(topics = "test-dlq", groupId = "test-dlq-group", containerFactory = "dlqKafkaListenerContainerFactory")
    public void listenDlq(EventWrapper<?> wrapper) {
        collect(wrapper);
    }

}
