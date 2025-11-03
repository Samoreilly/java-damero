package com.example.kafkaexample.service;

import net.damero.Kafka.CustomObject.EventWrapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class DLQMonitor {

    private static final Logger logger = LoggerFactory.getLogger(DLQMonitor.class);

    @KafkaListener(
        topics = "orders-dlq", 
        groupId = "dlq-monitor", 
        containerFactory = "dlqKafkaListenerContainerFactory"
    )
    public void monitorDLQ(ConsumerRecord<String, EventWrapper<?>> record) {
        try {
            EventWrapper<?> wrapper = record.value();
            
            // Safety check for null wrapper or metadata
            if (wrapper == null) {
                logger.warn("received null wrapper in dlq topic");
                return;
            }
            
            if (wrapper.getMetadata() == null) {
                logger.warn("received wrapper with null metadata in dlq topic");
                return;
            }
            
            logger.info("=== DLQ Message Received ===");
            logger.info("key: {}", record.key());
            logger.info("partition: {}, offset: {}", record.partition(), record.offset());
            
            // Safely log exception info
            if (wrapper.getMetadata().getLastFailureException() != null) {
                logger.info("exception: {}", wrapper.getMetadata().getLastFailureException().getClass().getSimpleName());
            } else {
                logger.info("exception: null");
            }
            
            logger.info("attempts: {}", wrapper.getMetadata().getAttempts());
            logger.info("original topic: {}", wrapper.getMetadata().getOriginalTopic());
            logger.info("dlq topic: {}", wrapper.getMetadata().getDlqTopic());
            logger.info("first failure: {}", wrapper.getMetadata().getFirstFailureDateTime());
            logger.info("last failure: {}", wrapper.getMetadata().getLastFailureDateTime());
            logger.info("event: {}", wrapper.getEvent());
            logger.info("===========================");
            
            // If attempts == 1, it was likely a non-retryable exception
            // If attempts == 3, it exhausted retries
            if (wrapper.getMetadata().getAttempts() == 1) {
                logger.info("this message went directly to dlq (non-retryable exception)");
            } else {
                logger.info("this message was retried {} times before going to dlq", wrapper.getMetadata().getAttempts());
            }
        } catch (Exception e) {
            logger.error("error processing dlq message at partition: {}, offset: {}", 
                record.partition(), record.offset(), e);
        }
    }
}

