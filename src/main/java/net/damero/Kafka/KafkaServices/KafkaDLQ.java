package net.damero.Kafka.KafkaServices;

import net.damero.Kafka.CustomObject.EventMetadata;
import net.damero.Kafka.CustomObject.EventWrapper;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import java.util.ArrayList;
import static net.damero.Kafka.CustomObject.GlobalExceptionMapLogger.exceptions;

@Slf4j
@Component
public class KafkaDLQ {

    //static method to call it avoid uneccessary injections
    @SuppressWarnings({"unchecked", "rawtypes"})
    public static void sendToDLQ(KafkaTemplate<?, ?> kafkaTemplate, String topic, EventWrapper<?> eventWrapper, Throwable throwable, EventMetadata eventMetadata){

        exceptions.computeIfAbsent(eventWrapper, k -> new ArrayList<>()).add(throwable);

        try {
            System.out.println("KafkaDLQ: Sending to topic: " + topic);
            ((KafkaTemplate) kafkaTemplate).send(topic, eventWrapper);
            System.out.println("KafkaDLQ: Successfully sent to " + topic);
        } catch (Exception e) {
            System.err.println("KafkaDLQ: Failed to send to " + topic + ": " + e.getMessage());
            e.printStackTrace();
        }
    }
}
