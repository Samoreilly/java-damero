package net.damero.Kafka.DeadLetterQueueAPI.ReadFromDLQ;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import net.damero.Kafka.CustomObject.EventWrapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.*;

@Component
public class ReadFromDLQConsumer{


    private final ConsumerFactory<String, EventWrapper<?>> dlqConsumerFactory;
    private final ObjectMapper kafkaObjectMapper;

    public ReadFromDLQConsumer(ConsumerFactory<String, EventWrapper<?>> dlqConsumerFactory,
                               ObjectMapper kafkaObjectMapper) {
        this.dlqConsumerFactory = dlqConsumerFactory;
        this.kafkaObjectMapper = kafkaObjectMapper;
    }

    public List<EventWrapper<?>> readFromDLQ(String topic){

        List<EventWrapper<?>> events = new ArrayList<>();

        Map<String, Object> consumerProps = new HashMap<>(dlqConsumerFactory.getConfigurationProperties());
        consumerProps.put("key.deserializer", StringDeserializer.class.getName());
        consumerProps.put("value.deserializer", StringDeserializer.class.getName());
        consumerProps.put("enable.auto.commit", false);
        consumerProps.put("auto.offset.reset", "earliest");
        consumerProps.put("group.id", "dlq-reader-" + UUID.randomUUID());

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps)) {
            consumer.subscribe(List.of(topic));

            boolean done = false;
            while (!done) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                if (records.isEmpty()) {
                    done = true; // stop when no more messages
                } else {
                    for (ConsumerRecord<String, String> record : records) {
                        EventWrapper<?> event = kafkaObjectMapper.readValue(record.value(), EventWrapper.class);
                        events.add(event);
                    }
                }
            }
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
        return events;
    }
}
