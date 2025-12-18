package net.damero.Kafka.KafkaServices;

import net.damero.Kafka.CustomObject.EventWrapper;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.springframework.kafka.core.KafkaTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import java.nio.charset.StandardCharsets;

@Service
public class KafkaDLQ {

    private static final Logger logger = LoggerFactory.getLogger(KafkaDLQ.class);

    // static method to call it avoid uneccessary injections
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public static void sendToDLQ(KafkaTemplate<?, ?> kafkaTemplate, String topic, EventWrapper<?> eventWrapper) {

        logger.debug("sending to dlq topic: {}", topic);
        try {
            logger.debug("sending to dlq topic: {}", topic);

            // Create headers and include __TypeId__ for the EventWrapper to help Spring
            // Kafka JsonDeserializer
            RecordHeaders headers = new RecordHeaders();
            headers.add("__TypeId__", EventWrapper.class.getName().getBytes(StandardCharsets.UTF_8));
            logger.debug("added __TypeId__ header for DLQ with value: {}", EventWrapper.class.getName());

            ProducerRecord<String, Object> record = new ProducerRecord<>(topic, null, null, null, eventWrapper,
                    headers);
            ((KafkaTemplate) kafkaTemplate).send(record);
            logger.info("successfully sent to dlq topic: {}", topic);
        } catch (Exception e) {
            logger.error("failed to send to dlq topic: {} with exception: {}", topic, e.getMessage(), e);
            throw new RuntimeException("failed to send to dlq topic: " + topic, e);
        }
    }
}
