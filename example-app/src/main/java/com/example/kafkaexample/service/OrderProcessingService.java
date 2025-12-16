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

    @DameroKafkaListener(
        topic = "orders",
        dlqTopic = "test-dlq",
        eventType =  com.example.kafkaexample.model.OrderEvent.class,
        maxAttempts = 3,
        delay = 1000,
        delayMethod = DelayMethod.FIBONACCI,
        fibonacciLimit = 15,
        nonRetryableExceptions = {
            IllegalArgumentException.class,
            ValidationException.class
        },
        deDuplication = false,
        openTelemetry = true,
        batchCapacity = 4000,
        batchWindowLength = 1000,
        fixedWindow = true

    )
    @KafkaListener(topics = "orders", groupId = "order-processor", containerFactory = "kafkaListenerContainerFactory")
    public void processOrder(ConsumerRecord<String, Object> record, Acknowledgment ack) {
        // With header-based approach, value is always the original OrderEvent
        // Metadata (attempts, failures, etc.) is stored in Kafka headers, not in the payload
        Object value = record.value();
        if (!(value instanceof OrderEvent order)) {
            logger.error("unexpected event type: {}", value != null ? value.getClass().getName() : "null");
            return;
        }
        
        // Check if this is a replayed message (for testing/demo purposes)
        boolean isReplay = record.headers().lastHeader("X-Replay-Mode") != null;

        if (isReplay) {
            logger.warn("REPLAY MODE: Skipping validation for order: {} (this is for testing only!)", order.getOrderId());
            logger.info("order {} processed successfully (validation skipped in replay mode)", order.getOrderId());
            ack.acknowledge();
            return;
        }

        logger.info("processing order: {}", order.getOrderId());

        // Validation checks - these exceptions are non-retryable
        if (order.getAmount() == null || order.getAmount() < 0) {
            throw new IllegalArgumentException("order amount cannot be null or negative: " + order.getAmount());
        }

        if (order.getCustomerId() == null || order.getCustomerId().isEmpty()) {
            throw new ValidationException("customer id is required");
        }

        // Payment processing - this exception is retryable (not in nonRetryableExceptions)
        if (order.getPaymentMethod() == null || order.getPaymentMethod().isEmpty()) {
            throw new PaymentException("payment method is required");
        }

        // Simulate network/database call that might fail
        if ("FAIL".equals(order.getStatus())) {
            throw new RuntimeException("simulated processing failure");
        }

        // Success
        logger.info("order {} processed successfully", order.getOrderId());
        ack.acknowledge();
    }
}

