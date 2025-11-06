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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Component
public class ReadFromDLQConsumer{


    private final ConsumerFactory<String, EventWrapper<?>> dlqConsumerFactory;
    private final ObjectMapper kafkaObjectMapper;
    private static final Logger logger = LoggerFactory.getLogger(ReadFromDLQConsumer.class);

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

            // wait until we get partition
            int assignmentAttempts = 0;
            while (consumer.assignment().isEmpty() && assignmentAttempts < 10) {
                consumer.poll(Duration.ofMillis(500));
                assignmentAttempts++;
            }
            
            // Seek to beginning if we have partitions
            if (!consumer.assignment().isEmpty()) {
                consumer.seekToBeginning(consumer.assignment());
            }
            
            boolean done = false;
            int emptyPolls = 0;
            while (!done && emptyPolls < 3) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                if (records.isEmpty()) {
                    emptyPolls++;
                } else {
                    emptyPolls = 0; // reset counter if we got messages
                    for (ConsumerRecord<String, String> record : records) {
                        try {
                            EventWrapper<?> event = kafkaObjectMapper.readValue(record.value(), EventWrapper.class);
                            events.add(event);
                        } catch (JsonProcessingException e) {
                            logger.warn("Failed to deserialize message at offset {}: {}", record.offset(), e.getMessage());
                        }
                    }
                }
            }
            logger.info("Read from DLQ topic: {} and found {} events", topic, events.size());
        } catch (Exception e) {
            logger.error("Failed to read from DLQ topic: {} with Exception: {}", topic, e.getMessage(), e);
            throw new RuntimeException(e);
        }
        return events;
    }
}
