package com.example.kafkaexample.controller;

import com.example.kafkaexample.model.OrderEvent;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.web.bind.annotation.*;
import java.util.Random;
import java.util.UUID;
import java.security.SecureRandom;
import java.nio.charset.StandardCharsets;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.header.Headers;


@RestController
@RequestMapping("/test")
public class TestController {

    @Autowired
    private KafkaTemplate<String, OrderEvent> kafkaTemplate;

    private final Random random = new Random();

    // @GetMapping("/string")
    // public String testString() {

    // kafkaTemplate.send("orders", "stringy");
    // return "sent order with negative amount - should go directly to dlq
    // (non-retryable)";
    // }
    //

    @PostMapping("/order/non-retryable/illegal-argument")
    public String testIllegalArgumentException() {
        OrderEvent badOrder = new OrderEvent();
        badOrder.setOrderId("order-001");
        badOrder.setAmount(-100.0); // Negative amount triggers IllegalArgumentException
        badOrder.setCustomerId("customer-123");
        badOrder.setPaymentMethod("credit-card");
        badOrder.setStatus("PENDING");

        kafkaTemplate.send("orders", badOrder);
        return "sent order with negative amount - should go directly to dlq (non-retryable)";
    }

    @PostMapping("/order/non-retryable/validation")
    public String testValidationException() {
        OrderEvent badOrder = new OrderEvent();
        badOrder.setOrderId("order-002");
        badOrder.setAmount(100.0);
        badOrder.setCustomerId(null); // Null customer triggers ValidationException
        badOrder.setPaymentMethod("credit-card");
        badOrder.setStatus("PENDING");

        kafkaTemplate.send("orders", badOrder);
        return "sent order with null customer - should go directly to dlq (non-retryable)";
    }

    @PostMapping("/order/retryable/payment")
    public String testRetryablePaymentException() {
        OrderEvent order = new OrderEvent();
        order.setOrderId("order-003");
        order.setAmount(100.0);
        order.setCustomerId("customer-123");
        order.setPaymentMethod(null); // Null payment method triggers PaymentException (retryable)
        order.setStatus("PENDING");

        kafkaTemplate.send("orders", order);
        return "sent order with null payment method - should retry 3 times then go to dlq";
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
        for (int i = 0; i < 75000; i++) {
            final String uuid = UUID.randomUUID().toString();
            String orderId = "order-10" + uuid;
            OrderEvent order = new OrderEvent();
            order.setOrderId(orderId);
            order.setAmount(100.0);
            order.setCustomerId("customer-123");
            order.setPaymentMethod("credit-card");
            order.setStatus("PENDING");

            kafkaTemplate.send("orders", order);
            System.out.println("sent order: " + orderId);
        }
        return "sent valid order - should process successfully";
    }

    @PostMapping("/order/random")
    public String testRandomEvents(@RequestParam(defaultValue = "100") int count) {
        int successCount = 0;
        int illegalArgCount = 0;
        int validationCount = 0;
        int paymentCount = 0;
        int runtimeCount = 0;

        for (int i = 0; i < count; i++) {
            String orderId = "random-order-" + System.currentTimeMillis() + "-" + i;
            OrderEvent order = new OrderEvent();
            order.setOrderId(orderId);

            int scenario = random.nextInt(5);
            switch (scenario) {
                case 0: // Success
                    order.setAmount(100.0 + random.nextDouble() * 900);
                    order.setCustomerId("customer-" + random.nextInt(1000));
                    order.setPaymentMethod("credit-card");
                    order.setStatus("PENDING");
                    successCount++;
                    break;
                case 1: // IllegalArgumentException (negative amount)
                    order.setAmount(-50.0 - random.nextDouble() * 100);
                    order.setCustomerId("customer-" + random.nextInt(1000));
                    order.setPaymentMethod("credit-card");
                    order.setStatus("PENDING");
                    illegalArgCount++;
                    break;
                case 2: // ValidationException (null customer)
                    order.setAmount(100.0 + random.nextDouble() * 900);
                    order.setCustomerId(null);
                    order.setPaymentMethod("credit-card");
                    order.setStatus("PENDING");
                    validationCount++;
                    break;
                case 3: // PaymentException (null payment method)
                    order.setAmount(100.0 + random.nextDouble() * 900);
                    order.setCustomerId("customer-" + random.nextInt(1000));
                    order.setPaymentMethod(null);
                    order.setStatus("PENDING");
                    paymentCount++;
                    break;
                case 4: // RuntimeException (FAIL status)
                    order.setAmount(100.0 + random.nextDouble() * 900);
                    order.setCustomerId("customer-" + random.nextInt(1000));
                    order.setPaymentMethod("credit-card");
                    order.setStatus("FAIL");
                    runtimeCount++;
                    break;
            }

            kafkaTemplate.send("orders", order);
        }

        return String.format("sent %d random orders: %d success, %d illegal-arg, %d validation, %d payment, %d runtime",
                count, successCount, illegalArgCount, validationCount, paymentCount, runtimeCount);
    }

    @PostMapping("/order/custom")
    public String testCustomOrder(@RequestBody OrderEvent order) {
        kafkaTemplate.send("orders", order);
        return "sent custom order: " + order.getOrderId();
    }

    @Autowired
    private KafkaTemplate<String, Boolean> booleanKafkaTemplate;

    @Autowired
    private KafkaTemplate<String, Integer> integerKafkaTemplate;

    @Autowired
    private KafkaTemplate<String, Long> longKafkaTemplate;

    @Autowired
    private KafkaTemplate<String, Double> doubleKafkaTemplate;

    @Autowired
    private KafkaTemplate<String, Float> floatKafkaTemplate;

    @GetMapping("/primitives")
    public String testPrimitives() {
        booleanKafkaTemplate.send("test-bool", true);
        integerKafkaTemplate.send("test-int", 42);
        longKafkaTemplate.send("test-long", 123456789L);
        doubleKafkaTemplate.send("test-double", 7.89d);
        floatKafkaTemplate.send("test-float", 4.56f);
        return "sent primitives: bool, int, long, double: 7.89, float: 4.56";
    }

    @GetMapping("/burst")
    public String testBurst() {
        for (int i = 0; i < 75000; i++) {
            final String uuid = UUID.randomUUID().toString();
            String orderId = "order-10" + uuid;
            OrderEvent order = new OrderEvent();
            order.setOrderId(orderId);
            order.setAmount(100.0);
            order.setCustomerId("customer-123");
            order.setPaymentMethod("credit-card");
            order.setStatus("PENDING");

            kafkaTemplate.send("orders", order);
            System.out.println("sent order: " + orderId);
        }
        return "sent valid order - should process successfully";
    }

    @GetMapping("/below")
    public String testBelowBatchCapacity() {
        for (int i = 0; i < 1000; i++) {
            final String uuid = UUID.randomUUID().toString();
            String orderId = "order-10" + uuid;
            OrderEvent order = new OrderEvent();
            order.setOrderId(orderId);
            order.setAmount(100.0);
            order.setCustomerId("customer-123");
            order.setPaymentMethod("credit-card");
            order.setStatus("PENDING");
            kafkaTemplate.send("orders", order);
            System.out.println("sent order: " + orderId);
        }
        return "sent valid order - should process successfully";
    }

    @GetMapping("/duplicates")
    public String sendDuplicates() {
        OrderEvent order = new OrderEvent();
        order.setOrderId("1");
        order.setAmount(100.0);
        order.setCustomerId("customer-123");
        order.setPaymentMethod("credit-card");
        order.setStatus("PENDING");
    
        final String key = "1";
        final String eventId = UUID.randomUUID().toString();
    
        for (int i = 0; i < 1000; i++) {
            RecordHeaders headers = new RecordHeaders();
            headers.add("event-type", "1".getBytes(StandardCharsets.UTF_8));
            headers.add("__TypeId__", OrderEvent.class.getName().getBytes(StandardCharsets.UTF_8));
            headers.add("event-id", eventId.getBytes(StandardCharsets.UTF_8));
    
            ProducerRecord<String, OrderEvent> record = new ProducerRecord<>(
                "orders", null, key, order, headers
            );
    
            kafkaTemplate.send(record);
        }
    
        return "sent duplicates with consistent event-id";
    }
    
}
