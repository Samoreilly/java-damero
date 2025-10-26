package net.damero.KafkaServices;

import org.springframework.kafka.core.KafkaTemplate;

public class KafkaDLQ {

    //static method to call it avoid uneccessary injections
    @SuppressWarnings({"unchecked", "rawtypes"})
    public static void sendToDLQ(KafkaTemplate<?, ?> kafkaTemplate, String topic, Object message){
        ((KafkaTemplate) kafkaTemplate).send(topic, message);
    }
}
