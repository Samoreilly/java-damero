package net.damero.KafkaServices;

import net.damero.CustomObject.EventMetadata;
import net.damero.CustomObject.EventWrapper;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

import static net.damero.CustomObject.GlobalExceptionMapLogger.exceptions;

@Component
public class KafkaDLQ {

    //static method to call it avoid uneccessary injections
    @SuppressWarnings({"unchecked", "rawtypes"})
    public static void sendToDLQ(KafkaTemplate<?, ?> kafkaTemplate, String topic, EventWrapper eventWrapper, Throwable throwable, EventMetadata eventMetadata){

        exceptions.computeIfAbsent(eventWrapper, k -> new ArrayList<>()).add(throwable);

        try {
            System.out.println("💀 KafkaDLQ: Sending to topic: " + topic);
            ((KafkaTemplate) kafkaTemplate).send(topic, eventWrapper);
            System.out.println("✅ KafkaDLQ: Successfully sent to " + topic);
        } catch (Exception e) {
            System.err.println("❌ KafkaDLQ: Failed to send to " + topic + ": " + e.getMessage());
            e.printStackTrace();
        }
    }
}
