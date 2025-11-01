# Kafka Damero

Kafka Damero is a Spring Boot library that makes working with Kafka listeners easier. It adds automatic retry logic, dead letter queue handling, circuit breaker support, and metrics tracking with minimal configuration. The library is designed to be user friendly and requires only a simple annotation to get started.

## Status

This project is currently in active development. All features have been implemented and tested, but they have not been thoroughly tested in production environments yet. The library currently only supports Apache Kafka. Use at your own discretion.

## Features

Automatic retries with configurable attempts and delay strategies to handle transient failures gracefully.

Dead letter queue that automatically routes failed messages with full metadata tracking including attempt counts and failure timestamps.

Auto configuration with sensible defaults so you can get started quickly with minimal setup.

Flexible overrides allow you to customize any auto configured bean for your specific needs.

Metadata tracking captures when messages first failed, how many times they were retried, and what exceptions occurred.

Manual acknowledgment handling prevents duplicate processing automatically.

Circuit breaker support with optional Resilience4j integration for handling downstream service failures.

Metrics integration with optional Micrometer support for tracking processing times, success rates, and failure counts.

## How to Use the Library

The library is designed to be user friendly. You only need to add an annotation to your existing Kafka listener methods. Here is how it works:

### Step 1: Add the Dependency

```xml
<dependency>
    <groupId>java.damero</groupId>
    <artifactId>kafka-damero</artifactId>
    <version>0.1.0-SNAPSHOT</version>
</dependency>
```

### Step 2: Configure Kafka Properties

Add your Kafka bootstrap servers to application.properties:

```properties
spring.kafka.bootstrap-servers=localhost:9092
spring.kafka.consumer.group-id=my-app-group
spring.kafka.consumer.auto-offset-reset=earliest
```

Or in YAML:

```yaml
spring:
  kafka:
    bootstrap-servers: localhost:9092
    consumer:
      group-id: my-app-group
      auto-offset-reset: earliest
```

### Step 3: Create Your Listener

Add the CustomKafkaListener annotation to your existing KafkaListener method:

```java
@Component
public class OrderListener {

    @CustomKafkaListener(
        topic = "orders",
        dlqTopic = "orders-dlq",
        maxAttempts = 3,
        delay = 1000,
        delayMethod = DelayMethod.EXPO
    )
    @KafkaListener(
        topics = "orders",
        groupId = "order-group",
        containerFactory = "kafkaListenerContainerFactory"
    )
    public void listen(Object payload, Acknowledgment acknowledgment) {
        OrderEvent event = (OrderEvent) payload;
        process(event);
    }
}
```

That is it. The library handles retries, DLQ routing, and acknowledgment automatically.

## Quick Start Example

Here is a complete example showing how to set up the library:

```java
// 1. Your event class
public class OrderEvent {
    private String orderId;
    private String customerId;
    private double amount;
    // getters, setters, constructors
}

// 2. Kafka configuration (optional if using defaults)
@Configuration
@EnableKafka
public class KafkaConfig {
    
    @Bean
    public ConsumerFactory<String, OrderEvent> orderConsumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "order-service");
        
        return new DefaultKafkaConsumerFactory<>(
            props,
            new StringDeserializer(),
            new JsonDeserializer<>(OrderEvent.class)
        );
    }
    
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, OrderEvent> orderListenerFactory(
            ConsumerFactory<String, OrderEvent> orderConsumerFactory) {
        ConcurrentKafkaListenerContainerFactory<String, OrderEvent> factory = 
            new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(orderConsumerFactory);
        
        // required for retry handling to avoid duplicate processing
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
        factory.setCommonErrorHandler(null);
        
        return factory;
    }
}

// 3. Your listener with retry logic
@Component
public class OrderListener {
    
    @CustomKafkaListener(
        topic = "orders",
        dlqTopic = "orders-dlq",
        maxAttempts = 3,
        delay = 1000,
        delayMethod = DelayMethod.EXPO
    )
    @KafkaListener(
        topics = "orders",
        groupId = "order-service",
        containerFactory = "orderListenerFactory"
    )
    public void processOrder(OrderEvent event, Acknowledgment acknowledgment) {
        // your business logic here
        // if this throws an exception, the library handles retries automatically
        paymentService.processPayment(event);
    }
}

// 4. DLQ listener to monitor failures (optional)
@Component
public class OrderDLQListener {
    
    @KafkaListener(
        topics = "orders-dlq",
        groupId = "order-dlq-monitor",
        containerFactory = "dlqKafkaListenerContainerFactory"
    )
    public void handleFailedOrder(EventWrapper<?> wrapper) {
        OrderEvent failedOrder = (OrderEvent) wrapper.getEvent();
        EventMetadata metadata = wrapper.getMetadata();
        
        // send alert, log to monitoring system, trigger manual review, etc.
        alertService.sendAlert(
            "Order " + failedOrder.getOrderId() + " failed after " + 
            metadata.getAttempts() + " attempts"
        );
    }
}
```

## Configuration Options

### CustomKafkaListener Annotation

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| topic | String | Required | Source topic to consume from |
| dlqTopic | String | Required | Dead letter queue topic for failed messages |
| maxAttempts | int | 3 | Maximum retry attempts before sending to DLQ |
| delay | long | 1000 | Base delay in milliseconds between retries |
| delayMethod | DelayMethod | EXPO | Retry delay strategy |
| kafkaTemplate | Class | void.class | Custom KafkaTemplate bean class (optional) |
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

1. The library checks if the maximum number of attempts has been reached
2. If max attempts reached, the message is sent to the DLQ with full metadata
3. If max attempts not reached, the message is scheduled for retry with a configured delay
4. After the delay, the message is resent to the original topic wrapped in EventWrapper
5. The retry cycle continues until max attempts are reached or processing succeeds

### Key Points

Retries happen by resending messages to the original topic rather than consuming from Kafka repeatedly. This avoids duplicate processing.

Messages are only acknowledged after success or when max retries are reached. This prevents Kafka from redelivering messages.

DLQ messages include full retry history so you can understand what went wrong and why.

The aspect automatically unwraps ConsumerRecord and EventWrapper for you. You can accept ConsumerRecord directly if you prefer, and the library will extract the actual event before processing retry and DLQ logic.

## Auto Configuration

The library provides automatic configuration for common use cases. The following beans are auto configured and available immediately:

| Bean Name | Type | Purpose |
|-----------|------|---------|
| kafkaObjectMapper | ObjectMapper | JSON serialization with Java Time support and polymorphic type handling |
| defaultKafkaTemplate | KafkaTemplate | For sending messages to DLQ |
| dlqConsumerFactory | ConsumerFactory | Consumer factory for DLQ messages |
| dlqKafkaListenerContainerFactory | ConcurrentKafkaListenerContainerFactory | Container factory for DLQ listeners |
| caffeineCache | CaffeineCache | In memory cache for tracking retry attempts |

You do not need to configure DLQ serialization. The library handles it automatically.

You do not need to create templates for DLQ messages. A default template is provided.

You do not need to configure JSON serialization. An ObjectMapper is configured with appropriate settings.

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
    public void handleFailedOrder(EventWrapper<?> wrapper) {
        OrderEvent failedOrder = (OrderEvent) wrapper.getEvent();
        EventMetadata metadata = wrapper.getMetadata();
        
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
        
        // custom: ISO date format instead of timestamps
        mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        
        // required for EventWrapper deserialization
        BasicPolymorphicTypeValidator ptv = BasicPolymorphicTypeValidator.builder()
                .allowIfBaseType(Object.class)
                .build();
        mapper.activateDefaultTyping(ptv, ObjectMapper.DefaultTyping.NON_FINAL);
        
        return mapper;
    }
}
```

#### Override DLQ Consumer for Custom Group ID

```java
@Configuration
public class CustomKafkaConfig {
    
    @Bean
    public ConsumerFactory<String, EventWrapper<?>> dlqConsumerFactory(
            ObjectMapper kafkaObjectMapper) {
        
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "my-broker:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "my-custom-dlq-group");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        
        JsonDeserializer<EventWrapper<?>> deserializer = 
            new JsonDeserializer<>(EventWrapper.class, kafkaObjectMapper);
        deserializer.addTrustedPackages("com.mycompany.*");
        
        return new DefaultKafkaConsumerFactory<>(
            props,
            new StringDeserializer(),
            deserializer
        );
    }
}
```

#### Use Custom KafkaTemplate

```java
@Configuration
public class CustomKafkaConfig {
    
    @Bean
    public KafkaTemplate<String, OrderEvent> customOrderTemplate() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        
        ProducerFactory<String, OrderEvent> factory = 
            new DefaultKafkaProducerFactory<>(props);
        return new KafkaTemplate<>(factory);
    }
}

// use it in your listener
@CustomKafkaListener(
    topic = "orders",
    dlqTopic = "orders-dlq",
    kafkaTemplate = CustomOrderTemplate.class
)
@KafkaListener(topics = "orders", containerFactory = "orderListenerFactory")
public void processOrder(OrderEvent event, Acknowledgment ack) {
    // process order
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

### Monitor Your DLQ

Always set up a DLQ listener to alert operations team when messages fail, log to monitoring system like Datadog or New Relic, trigger manual review workflows, and collect metrics on failure patterns.

### Use Manual Acknowledgment

Always include the Acknowledgment parameter in your listener methods so the library can handle acknowledgment automatically and prevent duplicate processing.

### Container Factory Configuration

Always configure your container factory with manual acknowledgment mode and disable the common error handler to let the library handle retries:

```java
factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
factory.setCommonErrorHandler(null);
```

## Troubleshooting

### Messages Being Reprocessed After Max Attempts

If messages retry infinitely, ensure manual acknowledgment mode is enabled and that your listener method includes the Acknowledgment parameter. The library handles acknowledgment automatically.

### Deserialization Errors in DLQ

If you cannot deserialize messages from the DLQ, use the auto configured dlqKafkaListenerContainerFactory in your DLQ listener. This factory is configured to handle EventWrapper deserialization correctly.

### Custom ObjectMapper Not Being Used

If changes to ObjectMapper do not take effect, ensure your bean is named exactly kafkaObjectMapper. The library uses bean name matching for auto configuration.

### Auto Configuration Not Working

If beans are not found, check that custom.kafka.auto-config.enabled is not set to false in your application properties.

## Roadmap

Planned features for future releases:

- Async retry support for better performance
- Enhanced metrics and monitoring integration
- Conditional retry logic to retry only on specific exceptions
- Retry event hooks for custom logic
- Additional delay strategies

## Contributing

This project is in active development and I welcome collaborators. If you are interested in contributing, please feel free to submit issues and pull requests. The library is designed to be user friendly and contributions that improve ease of use are especially welcome.

Areas where contributions would be valuable:

- Additional messaging platform support beyond Kafka
- Performance improvements and optimizations
- Enhanced documentation and examples
- Additional retry strategies and delay methods
- Better error handling and recovery mechanisms
- Production readiness improvements

## License

[Your License Here]

## Support

For issues and questions:

- GitHub Issues: [Your Repo URL]
- Documentation: [Your Docs URL]