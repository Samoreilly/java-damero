package com.example.kafkaexample.service;

import com.example.kafkaexample.model.OrderEvent;
import com.example.kafkaexample.model.PaymentException;
import com.example.kafkaexample.model.ValidationException;
import net.damero.Kafka.Annotations.DameroKafkaListener;
import net.damero.Kafka.Config.DelayMethod;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;
import org.springframework.kafka.annotation.TopicPartition;

@Service
public class OrderProcessingService {

    private static final Logger logger = LoggerFactory.getLogger(OrderProcessingService.class);

    @DameroKafkaListener(topic = "orders", dlqTopic = "test-dlq", eventType = com.example.kafkaexample.model.OrderEvent.class, maxAttempts = 3, delay = 1000, delayMethod = DelayMethod.FIBONACCI, fibonacciLimit = 15, nonRetryableExceptions = {
            IllegalArgumentException.class,
            ValidationException.class
    }, deDuplication = false, openTelemetry = true, batchCapacity = 4000, batchWindowLength = 1000, fixedWindow = true

    )
    @KafkaListener(topics = "orders", groupId = "order-processor", containerFactory = "kafkaListenerContainerFactory")
    public void processOrder(ConsumerRecord<String, String> record, Acknowledgment ack) {
        // With header-based approach, value is always the original OrderEvent
        // Metadata (attempts, failures, etc.) is stored in Kafka headers, not in the
        // payload
        Object value = record.value();
        // if (!(value instanceof OrderEvent order) || !(value instanceof String order)
        // ) {
        // logger.error("unexpected event type: {}", value != null ?
        // value.getClass().getName() : "null");
        // return;
        // }

        // Check if this is a replayed message (for testing/demo purposes)
        boolean isReplay = record.headers().lastHeader("X-Replay-Mode") != null;

        if (value instanceof String) {
            if (isReplay) {
                logger.warn("REPLAY MODE: Skipping validation for order: {} (this is for testing only!)", value);
                logger.info("order {} processed successfully (validation skipped in replay mode)", value);
            }
            // logger.info("Received message: {}", value);
            ack.acknowledge();
            return;
        }

        // Validation checks - these exceptions are non-retryable

        if (value instanceof OrderEvent order) {
            if (isReplay) {
                logger.warn("REPLAY MODE: Skipping validation for order: {} (this is for testing only!)",
                        order.getOrderId());
                logger.info("order {} processed successfully (validation skipped in replay mode)", order.getOrderId());
                ack.acknowledge();
                return;
            }

            logger.info("processing order: {}", order.getOrderId());

            if (order.getAmount() == null || order.getAmount() < 0) {
                throw new IllegalArgumentException("order amount cannot be null or negative:" + order.getAmount());
            }

            if (order.getCustomerId() == null || order.getCustomerId().isEmpty()) {
                throw new ValidationException("customer id is required");
            }

            // Payment processing - this exception is retryable (not in
            // nonRetryableExceptions)
            if (order.getPaymentMethod() == null || order.getPaymentMethod().isEmpty()) {
                throw new PaymentException("payment method is required");
            }

            // Simulate network/database call that might fail
            if ("FAIL".equals(order.getStatus())) {
                throw new RuntimeException("simulated processing failure");
            }
        }

        ack.acknowledge();
    }

    @DameroKafkaListener(topic = "test-bool", dlqTopic = "bool-dlq")
    @KafkaListener(topics = "test-bool", groupId = "bool-processor")
    public void handleBoolean(Boolean value, Acknowledgment ack) {
        logger.info("Received Boolean: {}", value);
        ack.acknowledge();
    }

    @DameroKafkaListener(topic = "test-int", dlqTopic = "int-dlq")
    @KafkaListener(topics = "test-int", groupId = "int-processor")
    public void handleInteger(Integer value, Acknowledgment ack) {
        logger.info("Received Integer: {}", value);
        ack.acknowledge();
    }

    @DameroKafkaListener(topic = "test-long", dlqTopic = "long-dlq")
    @KafkaListener(topics = "test-long", groupId = "long-processor")
    public void handleLong(Long value, Acknowledgment ack) {
        logger.info("Received Long: {}", value);
        ack.acknowledge();
    }

    @DameroKafkaListener(topic = "test-double", dlqTopic = "double-dlq")
    @KafkaListener(topics = "test-double", groupId = "double-processor")
    public void handleDouble(Double value, Acknowledgment ack) {
        logger.info("Received Double: {}", value);
        ack.acknowledge();
    }

    @DameroKafkaListener(topic = "test-float", dlqTopic = "float-dlq")
    @KafkaListener(topics = "test-float", groupId = "float-processor")
    public void handleFloat(Float value, Acknowledgment ack) {
        logger.info("Received Float: {}", value);
        ack.acknowledge();
    }
}
