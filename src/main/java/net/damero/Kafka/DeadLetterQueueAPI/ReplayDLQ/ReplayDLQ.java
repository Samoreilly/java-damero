package net.damero.Kafka.DeadLetterQueueAPI.ReplayDLQ;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import net.damero.Kafka.CustomObject.EventWrapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.*;
import java.time.Duration;

@Component
public class ReplayDLQ {

    private static final Logger log = LoggerFactory.getLogger(ReplayDLQ.class);
    private static final long PARTITION_ASSIGNMENT_TIMEOUT_MS = 10_000;
    private static final long POLL_TIMEOUT_MS = 1_000;
    private static final int MAX_POLL_RECORDS = 500;

    private final ConsumerFactory<String, EventWrapper<?>> dlqConsumerFactory;

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final ObjectMapper kafkaObjectMapper;

    public ReplayDLQ(ConsumerFactory<String, EventWrapper<?>> dlqConsumerFactory,
                     KafkaTemplate<String, Object> kafkaTemplate,
                     ObjectMapper kafkaObjectMapper) {
        this.dlqConsumerFactory = dlqConsumerFactory;
        this.kafkaTemplate = kafkaTemplate;
        this.kafkaObjectMapper = kafkaObjectMapper;
    }

    /**
     * Replays all messages from a DLQ topic back to their original topics.
     *
     * @param dlqTopic the DLQ topic to replay messages from
     * @return ReplayResult containing statistics about the replay operation
     * @throws IllegalArgumentException if dlqTopic is null or empty
     */
    public ReplayResult replayMessages(String dlqTopic) {
        if (dlqTopic == null || dlqTopic.trim().isEmpty()) {
            throw new IllegalArgumentException("DLQ topic cannot be null or empty");
        }

        log.info("Starting replay from DLQ topic: {}", dlqTopic);

        Map<String, Object> consumerProps = createConsumerProperties();

        int successCount = 0;
        int failureCount = 0;
        Map<String, Integer> errorsPerTopic = new HashMap<>();

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps)) {
            consumer.subscribe(List.of(dlqTopic));

            // Wait for partition assignment
            if (!waitForPartitionAssignment(consumer, dlqTopic)) {
                throw new RuntimeException("Failed to assign partitions for topic: " + dlqTopic);
            }

            consumer.seekToBeginning(consumer.assignment());
            log.info("Consumer assigned to partitions: {}", consumer.assignment());

            boolean hasMoreRecords = true;
            while (hasMoreRecords) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(POLL_TIMEOUT_MS));

                if (records.isEmpty()) {
                    log.info("No more records found. Replay complete.");
                    hasMoreRecords = false;
                    break;
                }

                log.debug("Polled {} records from DLQ", records.count());

                for (ConsumerRecord<String, String> record : records) {
                    try {
                        EventWrapper<?> eventWrapper = kafkaObjectMapper.readValue(
                            record.value(),
                            EventWrapper.class
                        );

                        String originalTopic = extractOriginalTopic(eventWrapper, record);

                        if (originalTopic == null) {
                            log.warn("Could not determine original topic for record at offset {}", record.offset());
                            failureCount++;
                            continue;
                        }

                        kafkaTemplate.send(originalTopic, eventWrapper.getEvent()).get();
                        successCount++;

                        if (successCount % 100 == 0) {
                            log.info("Replayed {} messages successfully", successCount);
                        }

                    } catch (JsonProcessingException e) {
                        log.error("Failed to deserialize record at offset {}: {}", record.offset(), e.getMessage());
                        failureCount++;
                        errorsPerTopic.merge("deserialization_errors", 1, Integer::sum);
                    } catch (Exception e) {
                        log.error("Failed to replay record at offset {}: {}", record.offset(), e.getMessage(), e);
                        failureCount++;
                        errorsPerTopic.merge("send_errors", 1, Integer::sum);
                    }
                }

                // Commit offsets after processing batch
                try {
                    consumer.commitSync();
                    log.debug("Committed offsets for batch");
                } catch (Exception e) {
                    log.error("Failed to commit offsets: {}", e.getMessage());
                }
            }

        } catch (Exception e) {
            log.error("Error during DLQ replay: {}", e.getMessage(), e);
            throw new RuntimeException("DLQ replay failed", e);
        }

        ReplayResult result = new ReplayResult(successCount, failureCount, errorsPerTopic);
        log.info("Replay completed. Success: {}, Failures: {}", successCount, failureCount);
        return result;
    }

    private Map<String, Object> createConsumerProperties() {
        Map<String, Object> consumerProps = new HashMap<>(dlqConsumerFactory.getConfigurationProperties());
        consumerProps.put("key.deserializer", StringDeserializer.class.getName());
        consumerProps.put("value.deserializer", StringDeserializer.class.getName());
        consumerProps.put("enable.auto.commit", false);
        consumerProps.put("auto.offset.reset", "earliest");
        consumerProps.put("group.id", "dlq-replay-" + UUID.randomUUID());
        consumerProps.put("max.poll.records", MAX_POLL_RECORDS);
        return consumerProps;
    }

    private boolean waitForPartitionAssignment(KafkaConsumer<String, String> consumer, String topic) {
        long startTime = System.currentTimeMillis();

        while (consumer.assignment().isEmpty()) {
            if (System.currentTimeMillis() - startTime > PARTITION_ASSIGNMENT_TIMEOUT_MS) {
                log.error("Timeout waiting for partition assignment for topic: {}", topic);
                return false;
            }
            consumer.poll(Duration.ofMillis(100));
        }

        return true;
    }

    private String extractOriginalTopic(EventWrapper<?> eventWrapper, ConsumerRecord<String, String> record) {
        // Try to get original topic from EventWrapper metadata
        if (eventWrapper.getMetadata() != null && eventWrapper.getMetadata().getOriginalTopic() != null) {
            return eventWrapper.getMetadata().getOriginalTopic();
        }

        // Try to extract from headers
        if (record.headers() != null) {
            var originalTopicHeader = record.headers().lastHeader("original_topic");
            if (originalTopicHeader != null && originalTopicHeader.value() != null) {
                return new String(originalTopicHeader.value());
            }
        }

        log.warn("Could not determine original topic for record. EventWrapper or headers may be missing metadata.");
        return null;
    }

    /**
     * Result object containing statistics about a replay operation
     */
    public static class ReplayResult {
        private final int successCount;
        private final int failureCount;
        private final Map<String, Integer> errorsPerType;

        public ReplayResult(int successCount, int failureCount, Map<String, Integer> errorsPerType) {
            this.successCount = successCount;
            this.failureCount = failureCount;
            this.errorsPerType = new HashMap<>(errorsPerType);
        }

        public int getSuccessCount() {
            return successCount;
        }

        public int getFailureCount() {
            return failureCount;
        }

        public Map<String, Integer> getErrorsPerType() {
            return Collections.unmodifiableMap(errorsPerType);
        }

        public int getTotalProcessed() {
            return successCount + failureCount;
        }

        @Override
        public String toString() {
            return "ReplayResult{" +
                    "successCount=" + successCount +
                    ", failureCount=" + failureCount +
                    ", errorsPerType=" + errorsPerType +
                    '}';
        }
    }


}
