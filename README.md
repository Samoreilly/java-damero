
## PROJECT IS FULLY IN DEVELOPMENT AND FEATURES HAVE NOT BEEN VIGOROUSLY TESTED YET
# Kafka Damero 

A Spring Boot library that adds intelligent retry logic and dead letter queue (DLQ) handling to your Kafka listeners with minimal configuration
(in development)



## Features

**Automatic Retries** - Configurable retry attempts with multiple delay strategies  
**Dead Letter Queue** - Automatic DLQ routing for failed messages with metadata tracking  
**Auto-Configuration** - Zero config setup for most use cases  
**Flexible Overrides** - Override any bean for custom behavior
**Metadata Tracking** - Track attempt counts and failure timestamps  
**Manual Acknowledgment** - Proper message acknowledgment to prevent duplicate processing

---

## Quick Start

Most users need only:
- Set `spring.kafka.bootstrap-servers`
- Add a listener using the built-in `kafkaListenerContainerFactory`
- Annotate with `@CustomKafkaListener`

Minimal example

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
        containerFactory = "kafkaListenerContainerFactory" // provided by this library
    )
    public void listen(Object payload, Acknowledgment acknowledgment) {
        // The aspect unwraps ConsumerRecord/EventWrapper for you.
        OrderEvent event = (OrderEvent) payload;
        process(event);
    }
}
```

Notes
- You can also accept `ConsumerRecord<String, Object>`; the aspect unwraps it before retry/DLQ logic.
- On retry, the library resends `EventWrapper<?>` back to the original topic (metadata preserved).
- On max attempts, it sends one `EventWrapper<?>` to the DLQ. Use the provided `dlqKafkaListenerContainerFactory` to read it.

### 1. Add the Dependency

```xml
<dependency>
    <groupId>java.damero</groupId>
    <artifactId>demo</artifactId>
    <version>0.0.1-SNAPSHOT</version>
</dependency>
```

### 2. Configure Kafka Properties


#### YAML Configuration
```yaml
spring:
  kafka:
    bootstrap-servers: localhost:9092
    consumer:
      group-id: my-app-group
      auto-offset-reset: earliest
```

#### Application Properties
```application.properties
spring.kafka.bootstrap-servers=localhost:9092
spring.kafka.consumer.group-id=my-app-group
spring.kafka.consumer.auto-offset-reset=earliest
```


### 3. (Optional) Create Your Own Consumer Factory

```java
@Configuration
@EnableKafka
public class KafkaConfig {
    
    @Bean
    public ConsumerFactory<String, OrderEvent> orderConsumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "order-group");
        
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
        
        //required for retry handling to avoid duplicate processing
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
        factory.setCommonErrorHandler(null);
        
        return factory;
    }
}
```

### 4. Create Your Listener (alternative signature)

```java
@Component
public class OrderListener {

    @CustomKafkaListener(
        topic = "orders", //source topic to consume from
        dlqTopic = "orders-dlq", //topic to send failed messages to
        maxAttempts = 3,
        delay = 1000, // in milliseconds
        delayMethod = DelayMethod.EXPO //custom delay method
    )
    @KafkaListener(
        topics = "orders", //must match topic in @CustomKafkaListener
        groupId = "order-group",
        containerFactory = "orderListenerFactory"
    )
    public void processOrder(Object payload, Acknowledgment acknowledgment) {
        OrderEvent event = (OrderEvent) payload; // aspect ensures you get the actual event
        // your logic
        // if this throws an exception, the library handles retries automatically
        // Method is intercepted using AspectJ
        processPayment(event);
    }
}
```

**Important**: Always include the `Acknowledgment` parameter in your listener method signature. The library handles acknowledgment automatically.

---

## Configuration Options

### @CustomKafkaListener Annotation

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `topic` | String | Required | Source topic to consume from |
| `dlqTopic` | String | Required | Dead letter queue topic for failed messages |
| `maxAttempts` | int | 3 | Maximum retry attempts before sending to DLQ |
| `delay` | long | 1000 | Base delay in milliseconds between retries |
| `delayMethod` | DelayMethod | EXPO | Retry delay strategy |
| `kafkaTemplate` | Class | void.class | Custom KafkaTemplate bean class (optional) |

### Delay Methods

Choose how delays are calculated between retries:

| Method | Formula | Example (base=1000ms) |
|--------|---------|----------------------|
| **LINEAR** | `delay * attempts` | 1s, 2s, 3s, 4s... |
| **EXPO** | `delay * 2^attempts` | 1s, 2s, 4s, 8s... |
| **MAX** | Fixed maximum | 1s, 1s, 1s, 1s... |

---

## Auto-Configuration

The library provides **automatic configuration** for common use cases. The following beans are auto-configured and available immediately:

### Auto-Configured Beans

| Bean Name | Type | Purpose |
|-----------|------|---------|
| `kafkaObjectMapper` | `ObjectMapper` | JSON serialization with Java Time support and polymorphic type handling |
| `defaultKafkaTemplate` | `KafkaTemplate<String, Object>` | For sending messages to DLQ |
| `defaultFactory` | `ConcurrentKafkaListenerContainerFactory` | Default consumer factory (internal use) |
| `dlqConsumerFactory` | `ConsumerFactory<String, EventWrapper<?>>` | Consumer factory for DLQ messages |
| `dlqKafkaListenerContainerFactory` | `ConcurrentKafkaListenerContainerFactory` | Container factory for DLQ listeners |

### What This Means

#### You don't need to configure DLQ serialization  
#### You don't need to create templates for DLQ messages  
#### You don't need to configure JSON serialization  
#### There may be cases where you need to override auto-configured beans 


---

## Monitoring DLQ Messages

To monitor failed messages, create a DLQ listener:

```java
@Component
public class OrderDLQListener {
    
    @KafkaListener(
        topics = "orders-dlq",
        groupId = "order-dlq-monitor",
        containerFactory = "dlqKafkaListenerContainerFactory"  // Use auto-configured factory
    )
    public void handleFailedOrder(EventWrapper<?> wrapper) {
        OrderEvent failedOrder = (OrderEvent) wrapper.getEvent();
        EventMetadata metadata = wrapper.getMetadata();
        
        System.out.println("❌ Failed after " + metadata.getAttempts() + " attempts");
        System.out.println("First failure: " + metadata.getFirstFailureDateTime());
        System.out.println("Last failure: " + metadata.getLastFailureDateTime());
        System.out.println("Order ID: " + failedOrder.getId());
        
        // Send alert, log to monitoring system, trigger manual review, etc.
        alertOps(failedOrder, metadata);
    }
}
```

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
}
```

---

## Advanced Configuration

### Overriding Auto-Configured Beans

You can override any auto-configured bean by defining your own with the same name:

#### Override ObjectMapper for Custom Serialization

```java
@Configuration
public class CustomKafkaConfig {
    
    @Bean
    public ObjectMapper kafkaObjectMapper() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
        
        // Custom: ISO date format instead of timestamps
        mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        
        // Custom: Pretty print JSON in dev
        mapper.enable(SerializationFeature.INDENT_OUTPUT);
        
        // Required for EventWrapper deserialization
        BasicPolymorphicTypeValidator ptv = BasicPolymorphicTypeValidator.builder()
                .allowIfBaseType(Object.class)
                .build();
        mapper.activateDefaultTyping(ptv, ObjectMapper.DefaultTyping.NON_FINAL);
        
        return mapper;
    }
    
    // All other auto-configured beans will now use this ObjectMapper!
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
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "my-custom-dlq-group");  // Custom group
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
    
    // defaultKafkaTemplate and other beans still auto-configured!
}
```

#### Use Custom KafkaTemplate

```java
@Configuration
public class CustomKafkaConfig {
    
    @Bean
    public KafkaTemplate<String, OrderEvent> customOrderTemplate() {
        // Your custom producer configuration
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        
        ProducerFactory<String, OrderEvent> factory = 
            new DefaultKafkaProducerFactory<>(props);
        return new KafkaTemplate<>(factory);
    }
}

// Use it in your listener
@CustomKafkaListener(
    topic = "orders",
    dlqTopic = "orders-dlq",
    kafkaTemplate = CustomOrderTemplate.class  // Specify custom template
)
@KafkaListener(topics = "orders", containerFactory = "orderListenerFactory")
public void processOrder(OrderEvent event, Acknowledgment ack) {
    // Process order
}
```

### Disable Auto-Configuration

If you want complete control, disable auto-configuration:

```yaml
# application.yml
custom:
  kafka:
    auto-config:
      enabled: false
```
#### Application Properties
```application.properties
custom.kafka.auto-config.enabled=false
```


Then define all beans manually in your configuration.

---

## How It Works

### Retry Flow

```
Message Received
      ↓
   Process
      ↓
  Success? -> Yes -> Acknowledge & Done ✓
      ↓
     No
      ↓
  Attempt < Max? -> No -> Send to DLQ → Acknowledge
      ↓
     Yes
      ↓
   Wait (delay)
      ↓
  Retry Process
```

### Key Features

1. **In-Memory Retries**: Retries happen in the same thread without re consuming from Kafka
2. **Manual Acknowledgment**: Messages are only acknowledged after success or max retries reached
3. **No Duplicate Processing**: Proper acknowledgment prevents Kafka from redelivering messages
4. **Metadata Tracking**: DLQ messages include full retry history

---

## Example: Complete Setup

```java
// 1. Event Class
public class OrderEvent {
    private String orderId;
    private String customerId;
    private double amount;
    // Getters, setters, constructors
}

// 2. Kafka Configuration
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
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
        factory.setCommonErrorHandler(null);
        return factory;
    }
}

// 3. Order Listener (Main Topic)
@Component
public class OrderListener {
    
    @Autowired
    private PaymentService paymentService;
    
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
        // This will retry 3 times with exponential backoff (1s, 2s, 4s)
        paymentService.processPayment(event);
    }
}

// 4. DLQ Listener (Monitor Failures)
@Component
public class OrderDLQListener {
    
    @Autowired
    private AlertService alertService;
    
    @KafkaListener(
        topics = "orders-dlq",
        groupId = "order-dlq-monitor",
        containerFactory = "dlqKafkaListenerContainerFactory"
    )
    public void handleFailedOrder(EventWrapper<?> wrapper) {
        OrderEvent failedOrder = (OrderEvent) wrapper.getEvent();
        EventMetadata metadata = wrapper.getMetadata();
        
        alertService.sendAlert(
            "Order " + failedOrder.getOrderId() + " failed after " + 
            metadata.getAttempts() + " attempts"
        );
    }
}
```

---

## Requirements

- **Java**: 21 or higher
- **Spring Boot**: 3.x
- **Spring Kafka**: Compatible with Spring Boot 3.x

---

## Best Practices

### 1. Choose Appropriate Max Attempts
- **Transient failures** (network issues): 3-5 attempts
- **External API calls**: 3-4 attempts with exponential backoff
- **Database issues**: 2-3 attempts with linear backoff

### 2. Set Reasonable Delays
- **Fast retries**: 100-500ms base delay
- **External services**: 1000-2000ms base delay
- **Rate-limited APIs**: 5000ms+ base delay with exponential backoff

### 3. Monitor Your DLQ
Always set up a DLQ listener to:
- Alert operations team
- Log to monitoring system (Datadog, New Relic, etc.)
- Trigger manual review workflows
- Collect metrics on failure patterns

### 4. Use Manual Acknowledgment
Always include `Acknowledgment` parameter in your listener methods:
```java
public void listen(MyEvent event, Acknowledgment acknowledgment) {
    // The library handles acknowledgment automatically
}
```

### 5. Container Factory Configuration
Always configure your container factory with:
```java
factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
factory.setCommonErrorHandler(null);
```

---

## Troubleshooting

### Messages Being Reprocessed After Max Attempts
**Problem**: Messages retry infinitely  
**Solution**: Ensure manual ack mode is enabled and `Acknowledgment` parameter is in listener method

### Deserialization Errors in DLQ
**Problem**: Can't deserialize messages from DLQ  
**Solution**: Use the auto-configured `dlqKafkaListenerContainerFactory` in your DLQ listener

### Custom ObjectMapper Not Being Used
**Problem**: Changes to ObjectMapper don't take effect  
**Solution**: Ensure your bean is named exactly `kafkaObjectMapper`

### Auto-Configuration Not Working
**Problem**: Beans not found  
**Solution**: Check that `custom.kafka.auto-config.enabled` is not set to `false`

---

## Roadmap

- [ ] Async retry support
- [ ] Metrics and monitoring integration
- [ ] RabbitMQ support
- [ ] Conditional retry logic (retry only on specific exceptions)
- [ ] Retry event hooks for custom logic

---

## Contributing

Contributions are welcome! Please feel free to submit issues and pull requests.

---

## License

[Your License Here]

---

## Support

For issues and questions:
- GitHub Issues: [Your Repo URL]
- Documentation: [Your Docs URL]