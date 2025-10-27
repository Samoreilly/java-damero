package net.damero;

import net.damero.CustomObject.EventWrapper;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

@Component
public class DLQMessageCollector {

    private final List<Object> receivedMessages = new CopyOnWriteArrayList<>();
    private String topicName = "test-dlq";

    @KafkaListener(topics = "test-dlq", groupId = "dlq-collector", containerFactory = "dlqListenerContainerFactory")
    public void collectDLQMessage(Object message) {
        receivedMessages.add(message);
    }

    public List<Object> getReceivedMessages() {
        return new ArrayList<>(receivedMessages);
    }

    public String getTopicName() {
        return topicName;
    }

    public void reset() {
        receivedMessages.clear();
    }
}

