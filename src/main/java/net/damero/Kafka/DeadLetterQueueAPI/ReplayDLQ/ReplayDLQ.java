package net.damero.Kafka.DeadLetterQueueAPI.ReplayDLQ;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import net.damero.Kafka.CustomObject.EventWrapper;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DeleteRecordsResult;
import org.apache.kafka.clients.admin.RecordsToDelete;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaAdmin;
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
    private static final int MAX_EMPTY_POLLS = 3; // Number of empty polls before stopping

    private final ConsumerFactory<String, EventWrapper<?>> dlqConsumerFactory;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final ObjectMapper kafkaObjectMapper;
    private final KafkaAdmin kafkaAdmin;

    public ReplayDLQ(ConsumerFactory<String, EventWrapper<?>> dlqConsumerFactory,
                     KafkaTemplate<String, Object> kafkaTemplate,
                     ObjectMapper kafkaObjectMapper,
                     KafkaAdmin kafkaAdmin) {
        this.dlqConsumerFactory = dlqConsumerFactory;
        this.kafkaTemplate = kafkaTemplate;
        this.kafkaObjectMapper = kafkaObjectMapper;
        this.kafkaAdmin = kafkaAdmin;
    }

    /**
     * Replays all messages from a DLQ topic back to their original topics.
     * This method respects committed offsets to avoid replaying already-processed messages.
     *
     * @param dlqTopic the DLQ topic to replay messages from
     * @return ReplayResult containing statistics about the replay operation
     * @throws IllegalArgumentException if dlqTopic is null or empty
     */
    public ReplayResult replayMessages(String dlqTopic) {
        return replayMessages(dlqTopic, false, false);
    }

    /**
     * Replays all messages from a DLQ topic back to their original topics.
     *
     * @param dlqTopic the DLQ topic to replay messages from
     * @param forceFromBeginning if true, resets consumer to beginning regardless of committed offsets (use for testing only)
     * @return ReplayResult containing statistics about the replay operation
     * @throws IllegalArgumentException if dlqTopic is null or empty
     */
    public ReplayResult replayMessages(String dlqTopic, boolean forceFromBeginning) {
        return replayMessages(dlqTopic, forceFromBeginning, false);
    }

    /**
     * Replays all messages from a DLQ topic back to their original topics.
     *
     * @param dlqTopic the DLQ topic to replay messages from
     * @param forceFromBeginning if true, resets consumer to beginning regardless of committed offsets (use for testing only)
     * @param skipValidation if true, adds X-Replay-Mode header to skip validation in consumer (for testing with invalid data)
     * @return ReplayResult containing statistics about the replay operation
     * @throws IllegalArgumentException if dlqTopic is null or empty
     */
    public ReplayResult replayMessages(String dlqTopic, boolean forceFromBeginning, boolean skipValidation) {
        if (dlqTopic == null || dlqTopic.trim().isEmpty()) {
            throw new IllegalArgumentException("DLQ topic cannot be null or empty");
        }

        log.info("Starting replay from DLQ topic: {} (forceFromBeginning={}, skipValidation={})",
                 dlqTopic, forceFromBeginning, skipValidation);

        Map<String, Object> consumerProps = createConsumerProperties(dlqTopic);

        int successCount = 0;
        int failureCount = 0;
        Map<String, Integer> errorsPerTopic = new HashMap<>();

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps)) {
            consumer.subscribe(List.of(dlqTopic));

            // Wait for partition assignment
            if (!waitForPartitionAssignment(consumer, dlqTopic)) {
                throw new RuntimeException("Failed to assign partitions for topic: " + dlqTopic);
            }

            // Only seek to beginning if explicitly requested (for testing)
            if (forceFromBeginning) {
                log.warn("Force replaying from beginning - this will reprocess already-replayed messages!");
                consumer.seekToBeginning(consumer.assignment());
            }

            log.info("Consumer assigned to partitions: {}", consumer.assignment());

            int emptyPollCount = 0;
            Set<String> processedOffsets = new HashSet<>();

            while (emptyPollCount < MAX_EMPTY_POLLS) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(POLL_TIMEOUT_MS));

                if (records.isEmpty()) {
                    emptyPollCount++;
                    log.debug("Empty poll {}/{}", emptyPollCount, MAX_EMPTY_POLLS);
                    continue;
                } else {
                    emptyPollCount = 0; // Reset counter when we get records
                }

                log.debug("Polled {} records from DLQ", records.count());

                for (ConsumerRecord<String, String> record : records) {
                    // Create unique identifier for this record
                    String recordId = record.topic() + "-" + record.partition() + "-" + record.offset();

                    // Skip if we've already processed this record
                    if (processedOffsets.contains(recordId)) {
                        log.debug("Skipping already processed record: {}", recordId);
                        continue;
                    }

                    try {
                        EventWrapper<?> eventWrapper = kafkaObjectMapper.readValue(
                            record.value(),
                            EventWrapper.class
                        );

                        String originalTopic = extractOriginalTopic(eventWrapper, record);

                        if (originalTopic == null) {
                            log.warn("Could not determine original topic for record at offset {}", record.offset());
                            failureCount++;
                            processedOffsets.add(recordId);
                            continue;
                        }

                        // Create ProducerRecord to add custom headers if needed
                        ProducerRecord<String, Object> producerRecord = new ProducerRecord<>(
                            originalTopic,
                            null,  // partition
                            null,  // key
                            eventWrapper.getEvent()
                        );

                        // Add replay mode header if skipValidation is enabled
                        if (skipValidation) {
                            producerRecord.headers().add(
                                new RecordHeader("X-Replay-Mode", "true".getBytes())
                            );
                        }

                        kafkaTemplate.send(producerRecord).get();
                        successCount++;
                        processedOffsets.add(recordId);

                        if (successCount % 100 == 0) {
                            log.info("Replayed {} messages successfully", successCount);
                        }

                    } catch (JsonProcessingException e) {
                        log.error("Failed to deserialize record at offset {}: {}", record.offset(), e.getMessage());
                        failureCount++;
                        processedOffsets.add(recordId);
                        errorsPerTopic.merge("deserialization_errors", 1, Integer::sum);
                    } catch (Exception e) {
                        log.error("Failed to replay record at offset {}: {}", record.offset(), e.getMessage(), e);
                        failureCount++;
                        processedOffsets.add(recordId);
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

            log.info("No more records found after {} empty polls. Replay complete.", MAX_EMPTY_POLLS);

            // Final commit to ensure all offsets are persisted
            try {
                consumer.commitSync();
                log.debug("Final commit completed");
            } catch (Exception e) {
                log.error("Failed to commit final offsets: {}", e.getMessage());
            }

            // Delete successfully replayed messages from DLQ
            if (successCount > 0) {
                try {
                    // Small delay to ensure commit has propagated
                    Thread.sleep(100);
                    deleteReplayedMessages(consumer, dlqTopic);
                } catch (Exception e) {
                    log.error("Failed to delete replayed messages from DLQ: {}", e.getMessage(), e);
                    // Don't throw - replay was successful, deletion is cleanup
                }
            } else {
                log.info("No messages were successfully replayed, skipping deletion");
            }

        } catch (Exception e) {
            log.error("Error during DLQ replay: {}", e.getMessage(), e);
            throw new RuntimeException("DLQ replay failed", e);
        }

        ReplayResult result = new ReplayResult(successCount, failureCount, errorsPerTopic);
        log.info("Replay completed. Success: {}, Failures: {}", successCount, failureCount);
        return result;
    }

    private Map<String, Object> createConsumerProperties(String dlqTopic) {
        Map<String, Object> consumerProps = new HashMap<>(dlqConsumerFactory.getConfigurationProperties());
        consumerProps.put("key.deserializer", StringDeserializer.class.getName());
        consumerProps.put("value.deserializer", StringDeserializer.class.getName());
        consumerProps.put("enable.auto.commit", false);
        consumerProps.put("auto.offset.reset", "earliest");
        // Use a consistent group ID per DLQ topic to track progress
        // This prevents re-reading messages that have already been replayed
        consumerProps.put("group.id", "dlq-replay-" + dlqTopic);
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

    /**
     * Deletes replayed messages from the DLQ by deleting records up to the committed offset.
     * This removes successfully replayed messages from the DLQ.
     */
    private void deleteReplayedMessages(KafkaConsumer<String, String> consumer, String dlqTopic) {
        log.info("Deleting successfully replayed messages from DLQ topic: {}", dlqTopic);

        try (AdminClient adminClient = AdminClient.create(kafkaAdmin.getConfigurationProperties())) {
            Map<TopicPartition, RecordsToDelete> recordsToDelete = new HashMap<>();

            // For each assigned partition, delete records up to and including the committed offset
            for (TopicPartition partition : consumer.assignment()) {
                try {
                    // Get the committed offset (offset of the last successfully processed message + 1)
                    var committedOffsetInfo = consumer.committed(Set.of(partition)).get(partition);
                    
                    if (committedOffsetInfo == null) {
                        log.warn("No committed offset found for partition {}", partition);
                        continue;
                    }
                    
                    long committedOffset = committedOffsetInfo.offset();
                    
                    // committedOffset is the next offset to read (one past the last processed)
                    // So we want to delete all records before this offset (which includes the last processed)
                    if (committedOffset > 0) {
                        // Delete all records up to and including the last processed message
                        // beforeOffset(committedOffset) deletes offsets < committedOffset
                        // So if committedOffset is 6, it deletes 0-5 (we processed 0-5, next read is 6)
                        recordsToDelete.put(partition, RecordsToDelete.beforeOffset(committedOffset));
                        log.info("Scheduling deletion of records up to (but not including) offset {} for partition {}",
                                 committedOffset, partition.partition());
                    } else if (committedOffset == -1) {
                        // No committed offset means we haven't processed anything yet
                        log.debug("No committed offset for partition {} - nothing to delete", partition);
                    }
                } catch (Exception e) {
                    log.error("Error getting committed offset for partition {}: {}", partition, e.getMessage());
                    // Continue with other partitions
                }
            }

            if (!recordsToDelete.isEmpty()) {
                DeleteRecordsResult result = adminClient.deleteRecords(recordsToDelete);
                result.all().get(); // Wait for deletion to complete
                log.info("Successfully deleted replayed messages from DLQ topic: {}", dlqTopic);
            } else {
                log.warn("No records to delete from DLQ topic: {} - check if messages were actually replayed", dlqTopic);
            }

        } catch (Exception e) {
            log.error("Failed to delete replayed messages from DLQ: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to delete replayed messages", e);
        }
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
