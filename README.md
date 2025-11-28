# Kafka Damero

A Spring Boot library that adds automatic retry logic, dead letter queue handling, and circuit breaker support to Kafka listeners. The library reduces boilerplate code and provides production-ready error handling with minimal configuration.

## Status

Beta release. All core features are implemented and tested. The library is functional and ready for testing in development environments. Use in production at your own discretion.

## Features

- Automatic retry logic with configurable backoff strategies (exponential, linear, Fibonacci, custom)
- Dead letter queue routing with complete metadata (attempts, timestamps, exceptions)
- Circuit breaker integration using Resilience4j
- Rate limiting to control message processing throughput
- Message deduplication to prevent duplicate processing
- Conditional DLQ routing to send different exceptions to different topics
- REST API for querying and replaying DLQ messages
- Distributed cache support using Redis for multi-instance deployments
- Metrics tracking with Micrometer
- Auto-configuration with sensible defaults

## Requirements

- Java 21 or higher
- Spring Boot 3.x
- Spring Kafka
- Apache Kafka

## Installation

Add the library to your project:

```xml
<dependency>
    <groupId>java.damero</groupId>
    <artifactId>kafka-damero</artifactId>
    <version>0.1.0-SNAPSHOT</version>
</dependency>
```

Required dependencies (these are marked as provided in the library, so you must include them):

```xml
<dependency>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-starter-aop</artifactId>
</dependency>
<dependency>
    <groupId>io.micrometer</groupId>
    <artifactId>micrometer-core</artifactId>
</dependency>
<dependency>
    <groupId>io.github.resilience4j</groupId>
    <artifactId>resilience4j-circuitbreaker</artifactId>
    <version>2.1.0</version>
</dependency>
<dependency>
    <groupId>com.github.ben-manes.caffeine</groupId>
    <artifactId>caffeine</artifactId>
</dependency>
```

Optional: Add Redis for distributed caching across multiple instances:

```xml
<dependency>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-starter-data-redis</artifactId>
</dependency>
```

Configure Kafka in your application.properties:

```properties
spring.kafka.bootstrap-servers=localhost:9092
```

If using Redis:

```properties
spring.data.redis.host=localhost
spring.data.redis.port=6379
```

## Quick Start

Create a listener with the @CustomKafkaListener annotation:

```java
@Service
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
        groupId = "order-processor",
        containerFactory = "kafkaListenerContainerFactory"
    )
    public void processOrder(ConsumerRecord<String, Object> record, Acknowledgment ack) {
        OrderEvent order = (OrderEvent) record.value();
        
        // Process the order
        processPayment(order);
        
        ack.acknowledge();
    }
}
```

The library handles retries, DLQ routing, and acknowledgment automatically.

## Configuration

### Annotation Parameters

The @CustomKafkaListener annotation supports these parameters:

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| topic | String | Required | Topic to consume from |
| dlqTopic | String | "" | Dead letter queue topic |
| maxAttempts | int | 3 | Maximum retry attempts |
| delay | double | 0.0 | Base delay in milliseconds |
| delayMethod | DelayMethod | EXPO | Retry backoff strategy |
| fibonacciLimit | int | 15 | Maximum Fibonacci sequence index |
| nonRetryableExceptions | Class[] | {} | Exceptions that skip retries |
| dlqRoutes | DlqExceptionRoutes[] | {} | Route specific exceptions to different DLQ topics |
| enableCircuitBreaker | boolean | false | Enable circuit breaker |
| circuitBreakerFailureThreshold | int | 50 | Failures before opening circuit |
| circuitBreakerWindowDuration | long | 60000 | Failure tracking window in ms |
| circuitBreakerWaitDuration | long | 60000 | Wait time before half-open in ms |
| messagesPerWindow | int | 0 | Rate limit: messages per window (0 = disabled) |
| messageWindow | long | 0 | Rate limit: window duration in ms |
| deDuplication | boolean | false | Enable message deduplication |

### Delay Methods

| Method | Formula | Example (delay=1000ms) |
|--------|---------|------------------------|
| EXPO | delay * 2^attempt | 1s, 2s, 4s, 8s |
| LINEAR | delay * attempt | 1s, 2s, 3s, 4s |
| FIBONACCI | Fibonacci sequence * delay | 1s, 1s, 2s, 3s, 5s |
| MAX | Fixed delay | 1s, 1s, 1s, 1s |

### Redis Configuration

The library uses Caffeine in-memory cache by default. For multi-instance deployments, add Redis:

1. Add spring-boot-starter-data-redis dependency
2. Configure Redis connection in application.properties

The library automatically detects Redis and uses it for distributed caching. If Redis becomes unavailable during runtime, all cache operations gracefully degrade:

- Cache writes fail silently without crashing the application
- Cache reads return false/null, which may cause duplicate processing
- Your application continues processing messages normally

This prevents Redis outages from bringing down your Kafka consumers. However, during a Redis outage, deduplication and retry tracking will not work across instances.

## How It Works

When a message fails processing:

1. The library checks if the exception is non-retryable
2. Non-retryable exceptions go directly to the DLQ
3. Retryable exceptions are checked against the max attempts limit
4. If max attempts reached, the message goes to DLQ with full metadata
5. Otherwise, the message is scheduled for retry based on the delay method
6. After the delay, the message is resent to the original topic
7. Retry metadata is stored in Kafka headers
8. The process repeats until success or max attempts reached

Key points:

- Retries happen by resending to the original topic, not by re-consuming
- Messages are acknowledged only after success or max retries
- DLQ messages include complete retry history
- Your listener receives the original event type, not a wrapper
- EventWrapper is only used for DLQ messages

## Auto-Configuration

The library auto-configures all necessary beans. You can override any bean by defining your own with the same name.

Key auto-configured beans:

- kafkaObjectMapper: JSON serialization with Java Time and polymorphic type support
- defaultKafkaTemplate: For sending messages to DLQ
- kafkaListenerContainerFactory: Container factory for listeners
- dlqKafkaListenerContainerFactory: Container factory for DLQ listeners
- pluggableRedisCache: Cache for retry tracking (Redis if available, otherwise Caffeine)
- kafkaListenerAspect: Intercepts @CustomKafkaListener methods
- retryOrchestrator: Manages retry logic
- dlqRouter: Routes messages to DLQ
- metricsRecorder: Tracks metrics if Micrometer is available

To disable auto-configuration:

```properties
custom.kafka.auto-config.enabled=false
```

## DLQ REST API

The library provides REST endpoints to query and replay DLQ messages.

### Available Endpoints

```
GET  /dlq?topic={dlq-topic}         # Enhanced view with statistics
GET  /dlq/stats?topic={dlq-topic}   # Statistics only
GET  /dlq/raw?topic={dlq-topic}     # Raw EventWrapper format
POST /dlq/replay/{topic}            # Replay messages to original topic
```

### Query Parameters for Replay

- forceReplay: If true, replays all messages from beginning. If false (default), only replays unprocessed messages.
- skipValidation: If true, adds X-Replay-Mode header to skip validation for testing with invalid data.

### Example Usage

```bash
# Query DLQ messages
curl http://localhost:8080/dlq?topic=orders-dlq

# Get statistics
curl http://localhost:8080/dlq/stats?topic=orders-dlq

# Replay messages
curl -X POST http://localhost:8080/dlq/replay/orders-dlq

# Force replay all messages
curl -X POST "http://localhost:8080/dlq/replay/orders-dlq?forceReplay=true"

# Replay with validation skipped
curl -X POST "http://localhost:8080/dlq/replay/orders-dlq?skipValidation=true"
```

The enhanced endpoint provides human-readable timestamps, calculated durations, severity classification (HIGH/MEDIUM/LOW based on failure count), exception type breakdown, and aggregate statistics.

## EventWrapper Structure

Messages in the DLQ are wrapped with metadata:

```java
public class EventWrapper<T> {
    private T event;                    // Your original event
    private LocalDateTime timestamp;    // When sent to DLQ
    private EventMetadata metadata;     // Retry information
}

public class EventMetadata {
    private LocalDateTime firstFailureDateTime;
    private LocalDateTime lastFailureDateTime;
    private int attempts;
    private String originalTopic;
    private String dlqTopic;
    private Exception firstFailureException;
    private Exception lastFailureException;
}
```

## Conditional DLQ Routing

Route different exceptions to different DLQ topics:

```java
@CustomKafkaListener(
    topic = "orders",
    dlqRoutes = {
        @DlqExceptionRoutes(
            exception = ValidationException.class,
            dlqExceptionTopic = "orders-validation-dlq",
            skipRetry = true
        ),
        @DlqExceptionRoutes(
            exception = TimeoutException.class,
            dlqExceptionTopic = "orders-timeout-dlq",
            skipRetry = false
        )
    }
)
```

Use skipRetry=true for validation errors that should not be retried. Use skipRetry=false for transient errors that should retry first.

## Advanced Features

### Rate Limiting

Limit message processing throughput:

```java
@CustomKafkaListener(
    topic = "orders",
    messagesPerWindow = 100,
    messageWindow = 60000  // 100 messages per minute
)
```

### Deduplication

Prevent duplicate message processing:

```java
@CustomKafkaListener(
    topic = "orders",
    deDuplication = true
)
```

The library uses message keys or content hashing to detect duplicates. Deduplication state is stored in the cache (Redis or Caffeine) with automatic expiration.

Configure deduplication settings in application.properties:

```properties
custom.kafka.deduplication.enabled=true
custom.kafka.deduplication.window-duration=10
custom.kafka.deduplication.window-unit=HOURS
custom.kafka.deduplication.num-buckets=1000
custom.kafka.deduplication.max-entries-per-bucket=50000
```

### Circuit Breaker

Protect downstream services:

```java
@CustomKafkaListener(
    topic = "orders",
    enableCircuitBreaker = true,
    circuitBreakerFailureThreshold = 50,
    circuitBreakerWindowDuration = 60000,
    circuitBreakerWaitDuration = 60000
)
```

When the circuit opens, messages go directly to DLQ without retries.

### Metrics

The library automatically records metrics if Micrometer is available:

- kafka.damero.processing.time: Processing time per message
- kafka.damero.processing.count: Success and failure counts
- kafka.damero.exception.count: Exception counts by type
- kafka.damero.retry.count: Retry attempts by topic

Metrics include tags for topic, status, exception type, and attempt number.

## Best Practices

### Max Attempts

- Transient failures (network issues): 3-5 attempts
- External API calls: 3-4 attempts with exponential backoff
- Database connections: 2-3 attempts with linear backoff

### Delays

- Fast retries for brief outages: 100-500ms base delay
- External services: 1000-2000ms base delay
- Rate limited APIs: 5000ms+ with exponential backoff

### Non-Retryable Exceptions

Mark validation and business logic errors as non-retryable:

```java
@CustomKafkaListener(
    topic = "orders",
    nonRetryableExceptions = {
        IllegalArgumentException.class,
        ValidationException.class
    }
)
```

These go directly to DLQ for manual review.

### Manual Acknowledgment

Always use manual acknowledgment mode:

```java
public void processOrder(ConsumerRecord<String, Object> record, Acknowledgment ack) {
    // Process message
    ack.acknowledge();
}
```

The library handles acknowledgment timing to prevent duplicate processing.

## Troubleshooting

### Messages retry infinitely

Ensure manual acknowledgment mode is enabled and your listener includes the Acknowledgment parameter.

### Cannot deserialize DLQ messages

Use the dlqKafkaListenerContainerFactory for DLQ listeners. It handles EventWrapper deserialization.

### Auto-configuration not working

Check that custom.kafka.auto-config.enabled is not set to false. Verify your application scans the correct packages.

## Contributing

Contributions are welcome. Submit issues and pull requests on GitHub.

Areas for contribution:

- Additional retry strategies
- Performance optimizations
- Enhanced documentation
- Production readiness improvements
- Bug fixes and tests

## License

See LICENSE file for details.
