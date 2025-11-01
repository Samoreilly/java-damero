package net.damero.Kafka.KafkaServices;

import net.damero.Kafka.CustomObject.EventMetadata;
import net.damero.Kafka.CustomObject.EventWrapper;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import java.util.ArrayList;
import static net.damero.Kafka.CustomObject.GlobalExceptionMapLogger.exceptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Component
public class KafkaDLQ {

    private static final Logger logger = LoggerFactory.getLogger(KafkaDLQ.class);
    //static method to call it avoid uneccessary injections
    @SuppressWarnings({"unchecked", "rawtypes"})
    public static void sendToDLQ(KafkaTemplate<?, ?> kafkaTemplate, String topic, EventWrapper<?> eventWrapper, Throwable throwable, EventMetadata eventMetadata){


        exceptions.computeIfAbsent(eventWrapper, k -> new ArrayList<>()).add(throwable);

        try {
            System.out.println("KafkaDLQ: Sending to topic: " + topic);
            ((KafkaTemplate) kafkaTemplate).send(topic, eventWrapper);

            logger.info("KafkaDlq: Succesfully to DLQ topic: {}", topic);
            System.out.println("KafkaDLQ: Successfully sent to " + topic);
            
        } catch (Exception e) {
            logger.error("KafkaDlq: Failed to send to DLQ topic: {} with Exception: {}", topic, e.getMessage());
            System.err.println("KafkaDLQ: Failed to send to " + topic + ": " + e.getMessage());
            e.printStackTrace();
        }
    }
}
