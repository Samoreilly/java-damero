package com.example.kafkaexample.service;

import com.example.kafkaexample.model.OrderEvent;
import com.example.kafkaexample.model.PaymentException;
import com.example.kafkaexample.model.ValidationException;
import net.damero.Kafka.Annotations.CustomKafkaListener;
import net.damero.Kafka.Config.DelayMethod;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

@Service
public class OrderProcessingService {

    private static final Logger logger = LoggerFactory.getLogger(OrderProcessingService.class);

    @CustomKafkaListener(
        topic = "orders",
        dlqTopic = "orders-dlq",
        maxAttempts = 3,
        delay = 1000,
        delayMethod = DelayMethod.LINEAR,
        nonRetryableExceptions = {
            IllegalArgumentException.class,
            ValidationException.class
        }
    )
    @KafkaListener(topics = "orders", groupId = "order-processor", containerFactory = "kafkaListenerContainerFactory")
    public void processOrder(ConsumerRecord<String, OrderEvent> record, Acknowledgment ack) {
        OrderEvent order = record.value();
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

