# Kafka Damero

Kafka Damero is a Spring Boot library that simplifies Kafka listener error handling. It provides automatic retry logic, dead letter queue routing, circuit breaker support, and metrics tracking with minimal configuration required.

## Status

⚠️ **Beta Release** - All core features are implemented and tested (45 passing integration tests). The library is functional and ready for evaluation in non-critical environments. Community feedback and contributions are welcome!

**Production Use:** While the library handles common error scenarios correctly, it has limited real-world production usage. Use in production systems at your own discretion.

## Why Kafka Damero?

**Before Kafka Damero**, Kafka error handling meant:
- 100+ lines of boilerplate per listener
- Manual retry scheduling and state management  
- Custom DLQ routing logic
- No visibility into failed messages

**After Kafka Damero**:
- ✅ Single `@CustomKafkaListener` annotation
- ✅ Automatic retry with 4 backoff strategies (Exponential, Linear, Fibonacci, Custom)
- ✅ Smart DLQ routing with full metadata (attempts, timestamps, exceptions)
- ✅ Built-in circuit breaker support (Resilience4j)
- ✅ Free REST API for DLQ monitoring (`/dlq?topic=orders-dlq`)
- ✅ Rate limiting and deduplication out of the box
- ✅ Metrics integration (Micrometer/Prometheus)
- ✅ Conditional DLQ routing (route different exceptions to different topics)
- ✅ Zero configuration required—works with sensible defaults

## Requirements

- Java 21 or higher
- Spring Boot 3.x
- Spring Kafka compatible with Spring Boot 3.x
- Apache Kafka

## Installation

Add the dependency to your project:

```xml
<dependency>
    <groupId>java.damero</groupId>
    <artifactId>kafka-damero</artifactId>
    <version>0.1.0-SNAPSHOT</version>
</dependency>
```

Configure Kafka bootstrap servers in your application properties:

```properties
spring.kafka.bootstrap-servers=localhost:9092
```

## Quick Start

Here is a minimal working example:

```java
// Define your event class
public class OrderEvent {
    private String orderId;
    private String customerId;
    private Double amount;
    private String paymentMethod;
    private String status;
    
    // constructors, getters, setters
}

// Create your listener with retry logic
@Service
public class OrderListener {
    
    @CustomKafkaListener(
        topic = "orders",
        dlqTopic = "orders-dlq",
        maxAttempts = 3,
        delay = 1000,
        delayMethod = DelayMethod.LINEAR,
        nonRetryableExceptions = { IllegalArgumentException.class }
    )
    @KafkaListener(
        topics = "orders",
        groupId = "order-processor",
        containerFactory = "kafkaListenerContainerFactory"
    )
    public void processOrder(ConsumerRecord<String, Object> record, Acknowledgment ack) {
        Object value = record.value();
        
        if (!(value instanceof OrderEvent order)) {
            return;
        }
        
        // Your business logic here
        if (order.getAmount() < 0) {
            throw new IllegalArgumentException("Invalid amount");
        }
        
        processPayment(order);
        ack.acknowledge();
    }
}
```

The library automatically handles retries, DLQ routing, and message acknowledgment. No additional configuration classes are required.

## Configuration

### CustomKafkaListener Annotation Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| topic | String | Required | Source topic to consume from |
| dlqTopic | String | "" | Dead letter queue topic for failed messages |
| maxAttempts | int | 3 | Maximum retry attempts before sending to DLQ |
| delay | double | 0.0 | Base delay in milliseconds between retries |
| delayMethod | DelayMethod | EXPO | Retry delay strategy |
| nonRetryableExceptions | Class[] | {} | Exceptions that skip retries and go directly to DLQ |
| enableCircuitBreaker | boolean | false | Enable circuit breaker integration |
| circuitBreakerFailureThreshold | int | 50 | Number of failures before opening circuit |
| circuitBreakerWindowDuration | long | 60000 | Time window in milliseconds for tracking failures |
| circuitBreakerWaitDuration | long | 60000 | Wait duration before transitioning to half open state |

### Delay Methods

The library supports different delay strategies for retries:

| Method | Formula | Example (base=1000ms) |
|--------|---------|----------------------|
| LINEAR | delay * attempts | 1s, 2s, 3s, 4s |
| EXPO | delay * 2^attempts | 1s, 2s, 4s, 8s |
| MAX | Fixed maximum delay | 1s, 1s, 1s, 1s |
| CUSTOM | Uses configured delay value | Uses delay value directly |

## How It Works

When a message is received, it is processed by your listener method. If processing succeeds, the message is acknowledged and processing completes.

If processing fails with an exception:

1. The library checks if the exception is in the nonRetryableExceptions list
2. If the exception is non-retryable, the message goes directly to DLQ with attempts set to 1
3. If the exception is retryable, the library checks if maximum attempts have been reached
4. If max attempts are reached, the message is sent to DLQ wrapped in EventWrapper with full metadata
5. If max attempts are not reached, the message is scheduled for retry with the configured delay
6. After the delay, the message is resent to the original topic with retry metadata stored in Kafka headers
7. The retry cycle continues until max attempts are reached or processing succeeds

### Important Notes

- Retries happen by resending messages to the original topic rather than consuming from Kafka repeatedly. This avoids duplicate processing.
- Messages are only acknowledged after success or when max retries are reached. This prevents Kafka from redelivering messages.
- DLQ messages include full retry history so you can understand what went wrong and why.
- Non-retryable exceptions bypass retry logic entirely and go directly to the DLQ on first failure.
- Your listener always receives your original event type. Retry metadata is stored in Kafka headers, not in the message payload. EventWrapper is only used for DLQ messages.

## Auto Configuration

The library automatically configures all common use cases. The following beans are created automatically:

| Bean Name | Type | Purpose |
|-----------|------|---------|
| kafkaObjectMapper | ObjectMapper | JSON serialization with Java Time support and polymorphic type handling |
| defaultKafkaTemplate | KafkaTemplate | For sending messages to DLQ |
| kafkaListenerContainerFactory | ConcurrentKafkaListenerContainerFactory | Container factory configured for retries |
| dlqConsumerFactory | ConsumerFactory | Consumer factory for DLQ messages |
| dlqKafkaListenerContainerFactory | ConcurrentKafkaListenerContainerFactory | Container factory for DLQ listeners |
| caffeineCache | CaffeineCache | In-memory cache for tracking retry attempts |
| kafkaListenerAspect | KafkaListenerAspect | The aspect that intercepts your listeners |
| retryOrchestrator | RetryOrchestrator | Orchestrates retry logic and attempt tracking |
| dlqRouter | DLQRouter | Routes messages to DLQ with metadata |
| metricsRecorder | MetricsRecorder | Tracks metrics if Micrometer is available |

All beans are created automatically with sensible defaults. You may override any auto configured bean if you need custom behavior.

To disable auto configuration, set:

```properties
custom.kafka.auto-config.enabled=false
```

## Monitoring DLQ Messages

To monitor failed messages, create a DLQ listener:

```java
@Component
public class OrderDLQListener {
    
    @KafkaListener(
        topics = "orders-dlq",
        groupId = "order-dlq-monitor",
        containerFactory = "dlqKafkaListenerContainerFactory"
    )
    public void handleFailedOrder(net.damero.Kafka.CustomObject.EventWrapper<?> wrapper) {
        OrderEvent failedOrder = (OrderEvent) wrapper.getEvent();
        net.damero.Kafka.CustomObject.EventMetadata metadata = wrapper.getMetadata();
        
        System.out.println("Order " + failedOrder.getOrderId() + 
            " failed after " + metadata.getAttempts() + " attempts");
        
        // Send alert, log to monitoring system, trigger manual review, etc.
        alertOperations(failedOrder, metadata);
    }
}
```

### DLQ REST API (Auto-Provided)

The library automatically provides REST endpoints to query DLQ messages - no additional setup required!

**Available Endpoints:**

- `GET /dlq?topic={dlq-topic}` - Enhanced view with human-readable formatting and statistics
- `GET /dlq/stats?topic={dlq-topic}` - Summary statistics only
- `GET /dlq/raw?topic={dlq-topic}` - Raw EventWrapper format

**Example Usage:**

```bash
# Get enhanced view with stats
curl http://localhost:8080/dlq?topic=orders-dlq

# Get just statistics
curl http://localhost:8080/dlq/stats?topic=orders-dlq
```

The enhanced endpoint provides:
- Human-readable timestamps instead of arrays
- Calculated durations ("15 minutes in DLQ")
- Severity classification (HIGH/MEDIUM/LOW)
- Exception type breakdown
- Aggregate statistics across all events

See `DLQ_QUICKSTART.md` for complete documentation.

### EventWrapper Structure

Messages sent to the DLQ are wrapped with metadata:

```java
public class EventWrapper<T> {
    private T event;                      // Your original event
    private LocalDateTime timestamp;      // When it was sent to DLQ
    private EventMetadata metadata;       // Retry metadata
}

public class EventMetadata {
    private LocalDateTime firstFailureDateTime;  // When first attempt failed
    private LocalDateTime lastFailureDateTime;   // When last attempt failed
    private int attempts;                        // Total number of attempts
    private String originalTopic;                // Original topic name
    private String dlqTopic;                     // DLQ topic name
    private Exception firstFailureException;     // First exception encountered
    private Exception lastFailureException;      // Most recent exception
}
```

## Advanced Configuration

### Overriding Auto Configured Beans

You can override any auto configured bean by defining your own with the same name.

#### Override ObjectMapper for Custom Serialization

```java
@Configuration
public class CustomKafkaConfig {
    
    @Bean
    public ObjectMapper kafkaObjectMapper() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
        
        BasicPolymorphicTypeValidator ptv = BasicPolymorphicTypeValidator.builder()
                .allowIfBaseType(Object.class)
                .build();
        mapper.activateDefaultTyping(ptv, ObjectMapper.DefaultTyping.NON_FINAL);
        
        return mapper;
    }
}
```

## Best Practices

### Choose Appropriate Max Attempts

For transient failures like network issues, use 3 to 5 attempts. For external API calls that might be temporarily unavailable, use 3 to 4 attempts with exponential backoff. For database connection issues, use 2 to 3 attempts with linear backoff.

### Set Reasonable Delays

For fast retries during brief outages, use 100 to 500 millisecond base delay. For external services that might take time to recover, use 1000 to 2000 millisecond base delay. For rate limited APIs, use 5000 millisecond or higher base delay with exponential backoff.

### Use Non-Retryable Exceptions

Mark exceptions that represent data validation errors or business logic errors as non-retryable. These should go directly to DLQ for manual review. Examples include IllegalArgumentException, ValidationException, or custom business logic exceptions.

### Monitor Your DLQ

Always set up a DLQ listener to alert your operations team when messages fail, log to monitoring systems like Datadog or New Relic, trigger manual review workflows, and collect metrics on failure patterns.

### Use Manual Acknowledgment

Always include the Acknowledgment parameter in your listener methods so the library can handle acknowledgment automatically and prevent duplicate processing.

## Troubleshooting

### Messages Being Reprocessed After Max Attempts

If messages retry infinitely, ensure manual acknowledgment mode is enabled and that your listener method includes the Acknowledgment parameter. The library handles acknowledgment automatically.

### Deserialization Errors in DLQ

If you cannot deserialize messages from the DLQ, use the auto configured dlqKafkaListenerContainerFactory in your DLQ listener. This factory is configured to handle EventWrapper deserialization correctly.

### Auto Configuration Not Working

If beans are not found, check that custom.kafka.auto-config.enabled is not set to false in your application properties. Also verify that your application is scanning the correct package for Spring components.

## Contributing

This project is in active development and contributions are welcome. If you are interested in contributing, please submit issues and pull requests. The library is designed to be user friendly and contributions that improve ease of use are especially welcome.

Areas where contributions would be valuable:

- Additional messaging platform support beyond Kafka
- Performance improvements and optimizations
- Enhanced documentation and examples
- Additional retry strategies and delay methods
- Better error handling and recovery mechanisms
- Production readiness improvements
