# Kafka Damero (not ready for production yet)
A Spring Boot library that adds retry logic and dead letter queue handling to your Kafka listeners. This library makes it easy to handle message processing failures with automatic retries and dead letter queues.

## What This Library Does

This library helps you handle failed Kafka messages by:

- Retrying failed messages automatically
- Sending failed messages to a dead letter queue after retries are exhausted
- Supporting different retry delay strategies
- Allowing custom configuration per listener

## Getting Started

### Adding the Dependency

Add this library to your `pom.xml`:

```xml
<dependency>
    <groupId>java.damero</groupId>
    <artifactId>demo</artifactId>
    <version>0.0.1-SNAPSHOT</version>
</dependency>
```

You also need Spring Boot and Spring Kafka:

```xml
<dependency>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-starter-kafka</artifactId>
</dependency>
```

### Basic Usage

Use the `@CustomKafkaListener` annotation alongside the standard `@KafkaListener` on your listener methods:

```java
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import net.damero.Annotations.CustomKafkaListener;
import net.damero.CustomKafkaSetup.DelayMethod;

@Component
public class OrderListener {

    @CustomKafkaListener(
        topic = "orders",
        dlqTopic = "orders-dlq",
        maxAttempts = 3,
        delay = 100,
        delayMethod = DelayMethod.LINEAR
    )
    @KafkaListener(topics = "orders", groupId = "order-processor")
    public void processOrder(OrderEvent event) {
        // Your processing logic here
        // If an exception is thrown, the library will retry automatically
    }
}
```

## Configuration Options

The `@CustomKafkaListener` annotation has these options:

| Option | Required | Default | Description |
|--------|----------|---------|-------------|
| `topic` | Yes | - | The Kafka topic name |
| `dlqTopic` | No | "" | Dead letter queue topic name |
| `maxAttempts` | No | 3 | Number of retry attempts |
| `delay` | No | 0.0 | Base delay in milliseconds between retries |
| `delayMethod` | No | EXPO | How to calculate delay between retries |
| `kafkaTemplate` | No | void.class | Custom Kafka template (advanced) |
| `consumerFactory` | No | void.class | Custom consumer factory (advanced) |

## Retry Delay Methods

You can choose how long to wait between retries using the `delayMethod` option:

- **LINEAR**: Delay increases linearly (delay * attempt number)
- **EXPO**: Delay increases exponentially (delay * 2^attempt)
- **MAX**: Uses a fixed maximum delay
- **CUSTOM**: For custom delay logic

### Example with Different Delay Methods

```java
// Linear: 100ms, 200ms, 300ms...
@CustomKafkaListener(
    topic = "orders",
    maxAttempts = 3,
    delay = 100,
    delayMethod = DelayMethod.LINEAR
)

// Exponential: 100ms, 200ms, 400ms...
@CustomKafkaListener(
    topic = "orders",
    maxAttempts = 3,
    delay = 100,
    delayMethod = DelayMethod.EXPO
)

// Fixed maximum delay
@CustomKafkaListener(
    topic = "orders",
    maxAttempts = 3,
    delay = 1000,
    delayMethod = DelayMethod.MAX
)
```

## Important Notes

1. The `topic` in `@CustomKafkaListener` must match the topic in `@KafkaListener`
2. Both annotations are required on the listener method
3. The library only intercepts methods with both annotations
4. After `maxAttempts`, the message is sent to the dead letter queue

## How Retries Work

When a listener method throws an exception:

1. The library catches the exception
2. It waits based on your delay method
3. It retries the message processing
4. This repeats up to `maxAttempts` times
5. After all attempts fail, the message is sent to the dead letter queue

## Example Project Structure

Your application should look like this:

```
src/
  main/
    java/
      yourpackage/
        YourListener.java
      net/
        damero/  <- Library code (included in dependency)
```

## Requirements

- Java 21 or higher
- Spring Boot 3.x
- Spring Kafka
- Maven or Gradle

## Limitations

- Currently focuses on Kafka (RabbitMQ support planned)
- Must be used with Spring Boot applications
- Requires `@KafkaListener` alongside `@CustomKafkaListener`

## Contributing

This library is in active development. Contributions and feedback are welcome.
