package com.example.kafkaexample.controller;

import com.example.kafkaexample.model.OrderEvent;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/test")
public class TestController {

    @Autowired
    private KafkaTemplate<String, OrderEvent> kafkaTemplate;

    @PostMapping("/order/non-retryable/illegal-argument")
    public String testIllegalArgumentException() {
        OrderEvent badOrder = new OrderEvent();
        badOrder.setOrderId("order-001");
        badOrder.setAmount(-100.0); // This will trigger IllegalArgumentException
        badOrder.setCustomerId("customer-123");
        
        kafkaTemplate.send("orders", badOrder);
        return "sent order with negative amount - should go directly to dlq (non-retryable)";
    }

    @PostMapping("/order/non-retryable/validation")
    public String testValidationException() {
        OrderEvent badOrder = new OrderEvent();
        badOrder.setOrderId("order-002");
        badOrder.setAmount(100.0);
        badOrder.setCustomerId(null); // This will trigger ValidationException
        
        kafkaTemplate.send("orders", badOrder);
        return "sent order without customer id - should go directly to dlq (non-retryable)";
    }

    @PostMapping("/order/retryable/payment")
    public String testRetryablePaymentException() {
        OrderEvent order = new OrderEvent();
        order.setOrderId("order-003");
        order.setAmount(100.0);
        order.setCustomerId("customer-123");
        order.setPaymentMethod(null); // This will trigger PaymentException (retryable)
        
        kafkaTemplate.send("orders", order);
        return "sent order without payment method - should retry 3 times then go to dlq";
    }

    @PostMapping("/order/retryable/runtime")
    public String testRetryableRuntimeException() {
        OrderEvent order = new OrderEvent();
        order.setOrderId("order-004");
        order.setAmount(100.0);
        order.setCustomerId("customer-123");
        order.setPaymentMethod("credit-card");
        order.setStatus("FAIL"); // This will trigger RuntimeException (retryable)
        
        kafkaTemplate.send("orders", order);
        return "sent order with fail status - should retry 3 times then go to dlq";
    }

    @PostMapping("/order/success")
    public String testSuccess() {
        OrderEvent order = new OrderEvent();
        order.setOrderId("order-005");
        order.setAmount(100.0);
        order.setCustomerId("customer-123");
        order.setPaymentMethod("credit-card");
        order.setStatus("PENDING");
        
        kafkaTemplate.send("orders", order);
        return "sent valid order - should process successfully";
    }

    @PostMapping("/order/custom")
    public String testCustomOrder(@RequestBody OrderEvent order) {
        kafkaTemplate.send("orders", order);
        return "sent custom order: " + order.getOrderId();
    }
}

