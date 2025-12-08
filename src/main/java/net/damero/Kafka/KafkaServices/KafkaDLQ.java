package net.damero.Kafka.KafkaServices;

import net.damero.Kafka.CustomObject.EventWrapper;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class KafkaDLQ {

    private static final Logger logger = LoggerFactory.getLogger(KafkaDLQ.class);
    //static method to call it avoid uneccessary injections
    @SuppressWarnings({"unchecked", "rawtypes"})
    public static void sendToDLQ(KafkaTemplate<?, ?> kafkaTemplate, String topic, EventWrapper<?> eventWrapper){

        logger.debug("sending to dlq topic: {}", topic);
        try {
            logger.debug("sending to dlq topic: {}", topic);
            ((KafkaTemplate) kafkaTemplate).send(topic, eventWrapper);
            logger.info("successfully sent to dlq topic: {}", topic);
        } catch (Exception e) {
            logger.error("failed to send to dlq topic: {} with exception: {}", topic, e.getMessage(), e);
            throw new RuntimeException("failed to send to dlq topic: " + topic, e);
        }
    }
}
