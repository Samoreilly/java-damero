# Kafka Damero

Kafka Damero is a Spring Boot library that makes working with Kafka listeners easier. It adds automatic retry logic, dead letter queue handling, circuit breaker support, and metrics tracking with zero configuration required. Just add the dependency and use the annotation and the library handles everything else.

## Status

This project is currently in active development. All features have been implemented and tested, but they have not been thoroughly tested in production environments yet. The library currently only supports Apache Kafka. Use at your own discretion.

## Features

Automatic retries with configurable attempts and delay strategies to handle transient failures.

Dead letter queue that automatically routes failed messages with metadata tracking including attempt counts and failure timestamps.

Zero configuration setup that works out of the box with sensible defaults.

Flexible overrides allow you to customize any auto configured bean.

Metadata tracking captures when messages first failed, how many times they were retried, and what exceptions occurred.

Manual acknowledgment handling prevents duplicate processing.

Exception type based retry logic allows you to specify which exceptions should bypass retries and go directly to the DLQ.

Circuit breaker support with optional Resilience4j integration for handling downstream service failures.

Metrics integration with optional Micrometer support for tracking processing times, success rates, and failure counts.

## How to Use

The library is designed to be user friendly. You only need to add the dependency and the annotation. No configuration files required.

### Step 1: Add the Dependency

```xml
<dependency>
    <groupId>java.damero</groupId>
    <artifactId>kafka-damero</artifactId>
    <version>0.1.0-SNAPSHOT</version>
</dependency>
```

That is it. The library auto configures everything including the aspect, retry orchestration, DLQ handling, and all required beans.

### Step 2: Configure Kafka

Add your Kafka bootstrap servers to application.properties:

```properties
spring.kafka.bootstrap-servers=localhost:9092
```

### Step 3: Create Your Listener

Add the CustomKafkaListener annotation to your existing KafkaListener method:

```java
@Service
public class OrderListener {

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
    @KafkaListener(
        topics = "orders", 
        groupId = "order-processor",
        containerFactory = "kafkaListenerContainerFactory"
    )
    public void processOrder(ConsumerRecord<String, Object> record, Acknowledgment ack) {
        Object value = record.value();
        OrderEvent order;
        if (value instanceof OrderEvent oe) {
            order = oe;
        } else if (value instanceof net.damero.Kafka.CustomObject.EventWrapper<?> wrapper) {
            order = (OrderEvent) wrapper.getEvent();
        } else {
            return;
        }
        
        // your business logic here
        // if this throws an exception, the library handles retries automatically
        processOrder(order);
    }
}
```

That is it. The library handles retries, DLQ routing, and acknowledgment automatically.

## Quick Start

Here is a minimal working example:

```java
// Your event class
public class OrderEvent {
    private String orderId;
    private String customerId;
    private Double amount;
    private String paymentMethod;
    private String status;
    // getters, setters
}

// Your listener with retry logic
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
        // With header-based approach, value is always your original event type
        // Retry metadata (attempts, failures, etc.) is stored in Kafka headers, not in the payload
        Object value = record.value();
        if (!(value instanceof OrderEvent order)) {
            return; // unexpected event type
        }
        
        // business logic
        if (order.getAmount() < 0) {
            throw new IllegalArgumentException("invalid amount");
        }
        
        // process order
        processPayment(order);
        ack.acknowledge();
    }
}

// Monitor DLQ (optional)
@Component
public class OrderDLQListener {
    
    @KafkaListener(
        topics = "orders-dlq",
        groupId = "dlq-monitor",
        containerFactory = "dlqKafkaListenerContainerFactory"
    )
    public void handleFailedOrder(net.damero.Kafka.CustomObject.EventWrapper<?> wrapper) {
        OrderEvent failedOrder = (OrderEvent) wrapper.getEvent();
        net.damero.Kafka.CustomObject.EventMetadata metadata = wrapper.getMetadata();
        
        System.out.println("Order " + failedOrder.getOrderId() + 
            " failed after " + metadata.getAttempts() + " attempts");
    }
}
```

No configuration class needed. No EnableAspectJAutoProxy needed. No ComponentScan needed. It just works.

## Configuration Options

### CustomKafkaListener Annotation

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

You can choose how delays are calculated between retries:

| Method | Formula | Example (base=1000ms) |
|--------|---------|----------------------|
| LINEAR | delay multiplied by attempts | 1s, 2s, 3s, 4s |
| EXPO | delay multiplied by 2 to the power of attempts | 1s, 2s, 4s, 8s |
| MAX | Fixed maximum delay | 1s, 1s, 1s, 1s |
| CUSTOM | Uses configured delay value | Uses delay value directly |

## How It Works

When a message is received, it is processed by your listener method. If processing succeeds, the message is acknowledged and processing is complete.

If processing fails with an exception:

1. The library checks if the exception is in nonRetryableExceptions
2. If non retryable, the message goes directly to DLQ with attempts = 1
3. If retryable, the library checks if maximum attempts have been reached
4. If max attempts reached, the message is sent to DLQ wrapped in EventWrapper with full metadata
5. If max attempts not reached, the message is scheduled for retry with configured delay
6. After the delay, the message is resent to the original topic with retry metadata stored in Kafka headers
7. The retry cycle continues until max attempts are reached or processing succeeds

### Important Points

Retries happen by resending messages to the original topic rather than consuming from Kafka repeatedly. This avoids duplicate processing.

Messages are only acknowledged after success or when max retries are reached. This prevents Kafka from redelivering messages.

DLQ messages include full retry history so you can understand what went wrong and why.

Non retryable exceptions bypass retry logic entirely and go directly to the DLQ on first failure.

Your listener always receives your original event type (OrderEvent in this example). Retry metadata is stored in Kafka headers, not in the message payload. EventWrapper is only used for DLQ messages.

## Auto Configuration

The library automatically configures all common use cases. The following beans are created automatically:

| Bean Name | Type | Purpose |
|-----------|------|---------|
| kafkaObjectMapper | ObjectMapper | JSON serialization with Java Time support and polymorphic type handling |
| defaultKafkaTemplate | KafkaTemplate | For sending messages to DLQ |
| kafkaListenerContainerFactory | ConcurrentKafkaListenerContainerFactory | Container factory configured for retries |
| dlqConsumerFactory | ConsumerFactory | Consumer factory for DLQ messages |
| dlqKafkaListenerContainerFactory | ConcurrentKafkaListenerContainerFactory | Container factory for DLQ listeners |
| caffeineCache | CaffeineCache | In memory cache for tracking retry attempts |
| kafkaListenerAspect | KafkaListenerAspect | The aspect that intercepts your listeners |
| retryOrchestrator | RetryOrchestrator | Orchestrates retry logic and attempt tracking |
| dlqRouter | DLQRouter | Routes messages to DLQ with metadata |
| metricsRecorder | MetricsRecorder | Tracks metrics if Micrometer is available |

You do not need to configure anything. All beans are created automatically with sensible defaults.

You may override any auto configured bean if you need custom behavior.

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
        
        // send alert, log to monitoring system, trigger manual review, etc.
        alertOps(failedOrder, metadata);
    }
}
```

### EventWrapper Structure

Messages sent to the DLQ are wrapped with metadata:

```java
public class EventWrapper<T> {
    private T event;                      // your original event
    private LocalDateTime timestamp;      // when it was sent to DLQ
    private EventMetadata metadata;       // retry metadata
}

public class EventMetadata {
    private LocalDateTime firstFailureDateTime;  // when first attempt failed
    private LocalDateTime lastFailureDateTime;   // when last attempt failed
    private int attempts;                        // total number of attempts
    private String originalTopic;                // original topic name
    private String dlqTopic;                     // DLQ topic name
    private Exception firstFailureException;     // first exception encountered
    private Exception lastFailureException;      // most recent exception
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
        
        // required for EventWrapper deserialization
        BasicPolymorphicTypeValidator ptv = BasicPolymorphicTypeValidator.builder()
                .allowIfBaseType(Object.class)
                .build();
        mapper.activateDefaultTyping(ptv, ObjectMapper.DefaultTyping.NON_FINAL);
        
        return mapper;
    }
}
```

### Disable Auto Configuration

If you want complete control, you can disable auto configuration:

```yaml
custom:
  kafka:
    auto-config:
      enabled: false
```

Or in application properties:

```properties
custom.kafka.auto-config.enabled=false
```

Then define all beans manually in your configuration.

## Requirements

Java 21 or higher

Spring Boot 3.x

Spring Kafka compatible with Spring Boot 3.x

Apache Kafka (currently only Kafka is supported)

## Best Practices

### Choose Appropriate Max Attempts

For transient failures like network issues, use 3 to 5 attempts. For external API calls that might be temporarily unavailable, use 3 to 4 attempts with exponential backoff. For database connection issues, use 2 to 3 attempts with linear backoff.

### Set Reasonable Delays

For fast retries during brief outages, use 100 to 500 millisecond base delay. For external services that might take time to recover, use 1000 to 2000 millisecond base delay. For rate limited APIs, use 5000 millisecond or higher base delay with exponential backoff.

### Use Non Retryable Exceptions

Mark exceptions that represent data validation errors or business logic errors as non retryable. These should go directly to DLQ for manual review. Examples include IllegalArgumentException, ValidationException, or custom business logic exceptions.

### Monitor Your DLQ

Always set up a DLQ listener to alert operations team when messages fail, log to monitoring system like Datadog or New Relic, trigger manual review workflows, and collect metrics on failure patterns.

### Use Manual Acknowledgment

Always include the Acknowledgment parameter in your listener methods so the library can handle acknowledgment automatically and prevent duplicate processing.

## Troubleshooting

### Messages Being Reprocessed After Max Attempts

If messages retry infinitely, ensure manual acknowledgment mode is enabled and that your listener method includes the Acknowledgment parameter. The library handles acknowledgment automatically.

### Deserialization Errors in DLQ

If you cannot deserialize messages from the DLQ, use the auto configured dlqKafkaListenerContainerFactory in your DLQ listener. This factory is configured to handle EventWrapper deserialization correctly.

### Auto Configuration Not Working

If beans are not found, check that custom.kafka.auto-config.enabled is not set to false in your application properties.

## Contributing

This project is in active development and I welcome collaborators. If you are interested in contributing, please feel free to submit issues and pull requests. The library is designed to be user friendly and contributions that improve ease of use are especially welcome.

Areas where contributions would be valuable:

Additional messaging platform support beyond Kafka

Performance improvements and optimizations

Enhanced documentation and examples

Additional retry strategies and delay methods

Better error handling and recovery mechanisms

Production readiness improvements
