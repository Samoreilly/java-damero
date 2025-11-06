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
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/*
 * This class purpose is to feed data to the dlq controller, the data is polled from the queue and cached in messageCache
 * the cache is refreshed every 5 seconds and the max number of messages to cache is 1000
 * each topic is given a last refresh time, if the last refresh time is less than 5 seconds, the cache is returned
 * if the last refresh time is greater than 5 seconds, the cache is refreshed and the last refresh time is updated
 * this is to keep data fresh and avoid polling the queue too often
 * 
 */
@Component
public class ReadFromDLQConsumer{


    private final ConsumerFactory<String, EventWrapper<?>> dlqConsumerFactory;
    private final ObjectMapper kafkaObjectMapper;
    private static final Logger logger = LoggerFactory.getLogger(ReadFromDLQConsumer.class);
    
    //Topic to list of messages cache
    private final Map<String, List<EventWrapper<?>>> messageCache = new ConcurrentHashMap<>();

    //topic to last refresh time cache
    private final Map<String, Long> lastRefreshTime = new ConcurrentHashMap<>();

    //cache ttl in milliseconds
    private static final long CACHE_TTL_MS = 5000;

    //max number of messages to cache per topic
    private static final int MAX_CACHED_MESSAGES = 1000;

    public ReadFromDLQConsumer(ConsumerFactory<String, EventWrapper<?>> dlqConsumerFactory,
                               ObjectMapper kafkaObjectMapper) {
        this.dlqConsumerFactory = dlqConsumerFactory;
        this.kafkaObjectMapper = kafkaObjectMapper;
    }

    public List<EventWrapper<?>> readFromDLQ(String topic){
        // Check cache first
        long now = System.currentTimeMillis();
        Long lastRefresh = lastRefreshTime.get(topic);

        //if were still in the 5000ms cache window, return the cached data
        
        if (lastRefresh != null && (now - lastRefresh) < CACHE_TTL_MS) {
            List<EventWrapper<?>> cached = messageCache.get(topic);
            if (cached != null) {
                logger.debug("Returning cached DLQ messages for topic: {} ({} messages)", topic, cached.size());
                return new ArrayList<>(cached);
            }
        }
        //otherwise refresh the cache and return the new data
        return refreshCache(topic);
    }
    
    private synchronized List<EventWrapper<?>> refreshCache(String topic) {
        // Double-check after acquiring lock
        long now = System.currentTimeMillis();
        Long lastRefresh = lastRefreshTime.get(topic);
        if (lastRefresh != null && (now - lastRefresh) < CACHE_TTL_MS) {
            List<EventWrapper<?>> cached = messageCache.get(topic);
            if (cached != null) {
                return new ArrayList<>(cached);
            }
        }
        
        List<EventWrapper<?>> events = new ArrayList<>();

        Map<String, Object> consumerProps = new HashMap<>(dlqConsumerFactory.getConfigurationProperties());
        consumerProps.put("key.deserializer", StringDeserializer.class.getName());
        consumerProps.put("value.deserializer", StringDeserializer.class.getName());
        consumerProps.put("enable.auto.commit", false);
        consumerProps.put("auto.offset.reset", "latest"); // starts from latest so we get the latest messages
        consumerProps.put("group.id", "dlq-reader-" + UUID.randomUUID());

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps)) {
            consumer.subscribe(List.of(topic));

            // Wait for partition assignment - need to poll to trigger rebalance
            long startTime = System.currentTimeMillis();
            long timeout = 5000; // 5 second timeout
            boolean assigned = false;
            
            while (!assigned && (System.currentTimeMillis() - startTime) < timeout) {
                consumer.poll(Duration.ofMillis(1000)); // poll to trigger partition assignment
                if (!consumer.assignment().isEmpty()) {
                    assigned = true;
                }
            }
            
            if (consumer.assignment().isEmpty()) {
                logger.warn("No partitions assigned for topic: {} after {} ms timeout", topic, timeout);
                return new ArrayList<>();
            }
            
            // get end offsets and read backwards
            Map<TopicPartition, Long> endOffsets = consumer.endOffsets(consumer.assignment());
            // map to store the start offsets for each partition
            Map<TopicPartition, Long> startOffsets = new HashMap<>();
            
            for (Map.Entry<TopicPartition, Long> entry : endOffsets.entrySet()) {
                long endOffset = entry.getValue();
                long startOffset = Math.max(0, endOffset - MAX_CACHED_MESSAGES);
                startOffsets.put(entry.getKey(), startOffset);
            }
            
            // seek to start position
            for (Map.Entry<TopicPartition, Long> entry : startOffsets.entrySet()) {
                consumer.seek(entry.getKey(), entry.getValue());
            }
            
            // read messages
            int pollCount = 0;
            while (pollCount < 10) { 
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                if (records.isEmpty()) {
                    pollCount++;
                    if (pollCount >= 3) break; // Stop after 3 empty polls
                } else {
                    pollCount = 0; // Reset counter if we got messages
                    for (ConsumerRecord<String, String> record : records) {
                        if (events.size() >= MAX_CACHED_MESSAGES) break;
                        try {
                            EventWrapper<?> event = kafkaObjectMapper.readValue(record.value(), EventWrapper.class);
                            events.add(event);
                        } catch (JsonProcessingException e) {
                            logger.warn("Failed to deserialize message at offset {}: {}", record.offset(), e.getMessage());
                        }
                    }
                }
            }
            
            // show most recent
            Collections.reverse(events);
            
            // update cache
            messageCache.put(topic, events);
            //update the last refresh time
            lastRefreshTime.put(topic, System.currentTimeMillis());
            
            logger.info("Refreshed DLQ cache for topic: {} with {} messages", topic, events.size());
        } catch (Exception e) {
            logger.error("Failed to read from DLQ topic: {} with Exception: {}", topic, e.getMessage(), e);
            List<EventWrapper<?>> cached = messageCache.get(topic);
            if (cached != null) {
                logger.warn("Returning stale cache due to error");
                return new ArrayList<>(cached);
            }
            throw new RuntimeException(e);
        }
        return new ArrayList<>(events);
    }
}
