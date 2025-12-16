# Testing Non-Retryable Exceptions in Production

## Step 1: Configure Your Kafka Listener

Add `nonRetryableExceptions` to your `@CustomKafkaListener` annotation:

```java
package com.example.service;

import net.damero.Kafka.Annotations.DameroKafkaListener;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.stereotype.Service;

@Service
public class OrderProcessingService {

   @DameroKafkaListener(
           topic = "orders",
           dlqTopic = "orders-dlq",
           maxAttempts = 3,
           delay = 1000,
           delayMethod = DelayMethod.EXPO,
           nonRetryableExceptions = {
                   IllegalArgumentException.class,
                   ValidationException.class,
                   UnsupportedOperationException.class
           }
   )
   @KafkaListener(topics = "orders", groupId = "order-processor")
   public void processOrder(ConsumerRecord<String, OrderEvent> record, Acknowledgment ack) {
      OrderEvent order = record.value();

      // This will be retried if it fails
      if (order.getAmount() < 0) {
         throw new IllegalArgumentException("Order amount cannot be negative");
      }

      // This will be retried if it fails
      if (!isValidPayment(order)) {
         throw new PaymentException("Payment validation failed");
      }

      // Process the order
      orderService.process(order);
      ack.acknowledge();
   }

   private boolean isValidPayment(OrderEvent order) {
      // Simulate payment validation
      return order.getPaymentMethod() != null;
   }
}
```

## Step 2: Create Test Scenarios

### Scenario 1: Non-Retryable Exception (Goes Directly to DLQ)

Send a message that triggers `IllegalArgumentException`:

```java
@RestController
@RequestMapping("/test")
public class TestController {
    
    @Autowired
    private KafkaTemplate<String, OrderEvent> kafkaTemplate;
    
    @PostMapping("/test-non-retryable")
    public String testNonRetryable() {
        OrderEvent badOrder = new OrderEvent();
        badOrder.setAmount(-100); // This will trigger IllegalArgumentException
        
        kafkaTemplate.send("orders", badOrder);
        return "Sent bad order - should go directly to DLQ without retries";
    }
}
```

**Expected Behavior:**
- Message is processed once
- `IllegalArgumentException` is thrown
- Message goes directly to `orders-dlq` topic
- No retries occur
- Check logs: `"exception IllegalArgumentException is non-retryable for topic: orders - sending directly to dlq"`

### Scenario 2: Retryable Exception (Gets Retried)

Send a message that triggers a retryable exception:

```java
@PostMapping("/test-retryable")
public String testRetryable() {
    OrderEvent order = new OrderEvent();
    order.setAmount(100);
    order.setPaymentMethod(null); // This might trigger PaymentException
    
    kafkaTemplate.send("orders", order);
    return "Sent order - should retry if PaymentException occurs";
}
```

**Expected Behavior:**
- Message is processed
- If `PaymentException` occurs, it will be retried up to 3 times
- Only after max attempts, it goes to DLQ
- Check logs for retry attempts

## Step 3: Monitor and Verify

### Check Logs

Look for these log messages:

```bash
# Non-retryable exception (sent directly to DLQ)
INFO: exception IllegalArgumentException is non-retryable for topic: orders - sending directly to dlq

# Retryable exception (gets retried)
DEBUG: exception caught during processing for topic: orders: PaymentException
DEBUG: scheduled retry attempt 1 for event in topic: orders
DEBUG: scheduled retry attempt 2 for event in topic: orders
INFO: max attempts reached (3) for event in topic: orders - sending to dlq: orders-dlq
```

### Check DLQ Topic

Monitor the DLQ topic to see which messages ended up there:

```java
@Component
public class DLQMonitor {
    
    @KafkaListener(topics = "orders-dlq", groupId = "dlq-monitor")
    public void monitorDLQ(ConsumerRecord<String, EventWrapper<?>> record) {
        EventWrapper<?> wrapper = record.value();
        Exception exception = wrapper.getMetadata().getLastFailureException();
        int attempts = wrapper.getMetadata().getAttempts();
        
        System.out.println("DLQ Message:");
        System.out.println("  Exception: " + exception.getClass().getSimpleName());
        System.out.println("  Attempts: " + attempts);
        System.out.println("  Original Event: " + wrapper.getEvent());
        
        // If attempts == 1, it was likely a non-retryable exception
        // If attempts == 3, it exhausted retries
    }
}
```

### Use Kafka CLI Tools

```bash
# Check DLQ topic for messages
kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic orders-dlq \
  --from-beginning

# Check main topic for messages (should be empty after processing)
kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic orders \
  --from-beginning
```

## Step 4: Integration Test Example

```java
@SpringBootTest
@EmbeddedKafka(topics = {"orders", "orders-dlq"})
class OrderProcessingIntegrationTest {
    
    @Autowired
    private KafkaTemplate<String, OrderEvent> kafkaTemplate;
    
    @Autowired
    private DLQMessageCollector dlqCollector;
    
    @Test
    void testNonRetryableException_GoesDirectlyToDLQ() {
        // Given: An order that triggers IllegalArgumentException
        OrderEvent badOrder = new OrderEvent();
        badOrder.setAmount(-100);
        
        // When: Send to Kafka
        kafkaTemplate.send("orders", badOrder);
        
        // Then: Should go directly to DLQ with 1 attempt
        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            EventWrapper<?> dlqMessage = dlqCollector.getLastMessage();
            assertNotNull(dlqMessage);
            assertEquals(1, dlqMessage.getMetadata().getAttempts());
            assertTrue(dlqMessage.getMetadata().getLastFailureException() 
                instanceof IllegalArgumentException);
        });
    }
    
    @Test
    void testRetryableException_GetsRetried() {
        // Given: An order that triggers PaymentException (retryable)
        OrderEvent order = new OrderEvent();
        order.setAmount(100);
        order.setPaymentMethod(null);
        
        // When: Send to Kafka
        kafkaTemplate.send("orders", order);
        
        // Then: Should retry 3 times before going to DLQ
        await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> {
            EventWrapper<?> dlqMessage = dlqCollector.getLastMessage();
            assertNotNull(dlqMessage);
            assertEquals(3, dlqMessage.getMetadata().getAttempts());
        });
    }
}
```

## Step 5: Real-World Testing Strategy

1. **Start with a Test Environment**
   - Set up a separate Kafka cluster for testing
   - Use test topics and test consumers

2. **Create Test Messages**
   - Use a REST endpoint or admin panel to send test messages
   - Create messages that trigger different exception types

3. **Monitor Behavior**
   - Watch application logs in real-time
   - Check DLQ topic contents
   - Verify retry counts in EventMetadata

4. **Verify Metrics** (if using Micrometer)
   ```java
   // Check retry metrics
   Counter retryCounter = meterRegistry.counter("kafka.retry.count", "topic", "orders");
   Counter dlqCounter = meterRegistry.counter("kafka.dlq.count", "topic", "orders");
   ```

## Common Exception Types to Test

```java
nonRetryableExceptions = {
    // Validation errors - shouldn't retry
    IllegalArgumentException.class,
    ValidationException.class,
    NullPointerException.class,
    
    // Business logic errors - shouldn't retry
    UnsupportedOperationException.class,
    IllegalStateException.class,
    
    // But network/timeout errors - should retry
    // (Don't include these in nonRetryableExceptions)
    // SocketTimeoutException.class,
    // ConnectException.class,
}
```

## Debugging Tips

1. **Enable Debug Logging**
   ```properties
   logging.level.net.damero.Kafka.Aspect=DEBUG
   ```

2. **Check EventWrapper Metadata**
   - Inspect `EventMetadata` in DLQ messages
   - `attempts` field tells you if it was retried
   - `firstFailureException` vs `lastFailureException` shows exception history

3. **Use Kafka Tooling**
   - Kafka Tool, Kafka Manager, or Conduktor to inspect topics
   - Check message headers and payloads

4. **Add Custom Logging**
   ```java
   logger.info("Processing order: {}, Exception type: {}, Is retryable: {}", 
       order.getId(), 
       e.getClass().getSimpleName(),
       !isNonRetryableException(e, dameroKafkaListener));
   ```

