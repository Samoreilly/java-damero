package net.damero.KafkaServices;

import net.damero.CustomObject.EventWrapper;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.List;

public class KafkaDLQ {

    //static method to call it avoid uneccessary injections
    @SuppressWarnings({"unchecked", "rawtypes"})
    public static void sendToDLQ(KafkaTemplate<?, ?> kafkaTemplate, String topic, Object message, List<Throwable> throwable){
        //wrapped in a custom object to add metadata

        EventWrapper eventWrapper = new EventWrapper(message, throwable);
        ((KafkaTemplate) kafkaTemplate).send(topic, eventWrapper);
    }
}
