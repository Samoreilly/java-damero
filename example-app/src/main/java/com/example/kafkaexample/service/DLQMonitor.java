//package com.example.kafkaexample.service;
//
//import net.damero.Kafka.CustomObject.EventWrapper;
//import org.apache.kafka.clients.consumer.ConsumerRecord;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//import org.springframework.kafka.annotation.KafkaListener;
//import org.springframework.stereotype.Service;
//
//import java.util.ArrayList;
//import java.util.Collections;
//import java.util.List;
//
///**
// * OPTIONAL: Example DLQ Monitor for real-time failure notifications
// *
// * ⚠️ This class is NOT required to use kafka-damero!
// *
// * The library already provides REST endpoints to query DLQ messages:
// * - GET /dlq?topic=test-dlq        (enhanced view with stats)
// * - GET /dlq/stats?topic=test-dlq  (statistics only)
// * - GET /dlq/raw?topic=test-dlq    (raw format)
// *
// * This DLQMonitor is an OPTIONAL example showing how to:
// * ✅ React to failures in real-time (e.g., send Slack alerts)
// * ✅ Custom logging of failures
// * ✅ Trigger automated workflows
// * ✅ Send notifications to operations teams
// *
// * Only create a DLQ listener if you need real-time reactions.
// * For querying/viewing DLQ messagesc, use the built-in REST endpoints.
// */
//@Service
//public class DLQMonitor {
//
//    private static final Logger logger = LoggerFactory.getLogger(DLQMonitor.class);
//
//    /**
//     * NOTE: Storing messages in memory is just for demonstration purposes.
//     * In production, you would typically:
//     * - Send alerts to Slack/PagerDuty
//     * - Write to a database for analysis
//     * - Trigger automated remediation workflows
//     *
//     * Or just use the built-in REST endpoints: GET /dlq?topic=test-dlq
//     */
//    private final List<EventWrapper<?>> dlqMessages = Collections.synchronizedList(new ArrayList<>());
//
//    @KafkaListener(
//        topics = "test-dlq",
//        groupId = "dlq-monitor",
//        containerFactory = "dlqKafkaListenerContainerFactory"
//    )
//    public void monitorDLQ(ConsumerRecord<String, EventWrapper<?>> record) {
//        try {
//            EventWrapper<?> wrapper = record.value();
//
//            // Safety check for null wrapper or metadata
//            if (wrapper == null) {
//                logger.warn("received null wrapper in dlq topic");
//                return;
//            }
//
//            if (wrapper.getMetadata() == null) {
//                logger.warn("received wrapper with null metadata in dlq topic");
//                return;
//            }
//
//            logger.info("=== DLQ Message Received ===");
//            logger.info("key: {}", record.key());
//            logger.info("partition: {}, offset: {}", record.partition(), record.offset());
//
//            // Safely log exception info
//            if (wrapper.getMetadata().getLastFailureException() != null) {
//                logger.info("exception: {}", wrapper.getMetadata().getLastFailureException().getClass().getSimpleName());
//            } else {
//                logger.info("exception: null");
//            }
//
//            logger.info("attempts: {}", wrapper.getMetadata().getAttempts());
//            logger.info("original topic: {}", wrapper.getMetadata().getOriginalTopic());
//            logger.info("dlq topic: {}", wrapper.getMetadata().getDlqTopic());
//            logger.info("first failure: {}", wrapper.getMetadata().getFirstFailureDateTime());
//            logger.info("last failure: {}", wrapper.getMetadata().getLastFailureDateTime());
//            logger.info("event: {}", wrapper.getEvent());
//            logger.info("===========================");
//
//            // Store the message for the API endpoint
//            dlqMessages.add(wrapper);
//
//            // If attempts == 1, it was likely a non-retryable exception
//            // If attempts == 3, it exhausted retries
//            if (wrapper.getMetadata().getAttempts() == 1) {
//                logger.info("this message went directly to dlq (non-retryable exception)");
//            } else {
//                logger.info("this message was retried {} times before going to dlq", wrapper.getMetadata().getAttempts());
//            }
//        } catch (Exception e) {
//            logger.error("error processing dlq message at partition: {}, offset: {}",
//                record.partition(), record.offset(), e);
//        }
//    }
//
//    public List<EventWrapper<?>> getDlqMessages() {
//        return new ArrayList<>(dlqMessages);
//    }
//
//    public void clearDlqMessages() {
//        dlqMessages.clear();
//    }
//}
//
