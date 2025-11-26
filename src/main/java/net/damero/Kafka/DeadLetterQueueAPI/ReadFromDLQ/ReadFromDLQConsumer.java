package net.damero.Kafka.DeadLetterQueueAPI.ReadFromDLQ;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import net.damero.Kafka.CustomObject.EventWrapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.stereotype.Component;
import java.time.Duration;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simple DLQ consumer that reads messages from Dead Letter Queue topics.
 */
@Component
public class ReadFromDLQConsumer {

    private final ConsumerFactory<String, EventWrapper<?>> dlqConsumerFactory;
    private final ObjectMapper kafkaObjectMapper;
    private static final Logger logger = LoggerFactory.getLogger(ReadFromDLQConsumer.class);

    public ReadFromDLQConsumer(ConsumerFactory<String, EventWrapper<?>> dlqConsumerFactory,
                               ObjectMapper kafkaObjectMapper) {
        this.dlqConsumerFactory = dlqConsumerFactory;
        this.kafkaObjectMapper = kafkaObjectMapper;
    }

    /**
     * Read DLQ messages from the specified topic.
     */
    public List<EventWrapper<?>> readFromDLQ(String topic) {
        List<EventWrapper<?>> events = new ArrayList<>();

        Map<String, Object> consumerProps = new HashMap<>(dlqConsumerFactory.getConfigurationProperties());
        consumerProps.put("key.deserializer", StringDeserializer.class.getName());
        consumerProps.put("value.deserializer", StringDeserializer.class.getName());
        consumerProps.put("enable.auto.commit", false);
        consumerProps.put("auto.offset.reset", "earliest");
        consumerProps.put("group.id", "dlq-reader-" + UUID.randomUUID());

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps)) {
            consumer.subscribe(List.of(topic));

            // Wait for partition assignment
            long startTime = System.currentTimeMillis();
            long timeout = 5000;
            boolean assigned = false;

            while (!assigned && (System.currentTimeMillis() - startTime) < timeout) {
                consumer.poll(Duration.ofMillis(1000));
                if (!consumer.assignment().isEmpty()) {
                    assigned = true;
                }
            }

            if (consumer.assignment().isEmpty()) {
                logger.warn("No partitions assigned for topic: {} after timeout", topic);
                return new ArrayList<>();
            }

            // Seek to beginning
            consumer.seekToBeginning(consumer.assignment());

            // Read messages
            int emptyPollCount = 0;
            while (emptyPollCount < 3) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                if (records.isEmpty()) {
                    emptyPollCount++;
                } else {
                    emptyPollCount = 0;
                    for (ConsumerRecord<String, String> record : records) {
                        try {
                            EventWrapper<?> event = kafkaObjectMapper.readValue(record.value(), EventWrapper.class);
                            events.add(event);
                        } catch (JsonProcessingException e) {
                            logger.warn("Failed to deserialize message from topic {} at offset {}: {}",
                                topic, record.offset(), e.getMessage());
                        }
                    }
                }
            }

            logger.info("Read {} messages from DLQ topic: {}", events.size(), topic);

        } catch (Exception e) {
            logger.error("Failed to read from DLQ topic: {}", topic, e);
            return new ArrayList<>();
        }

        return events;
    }
}

