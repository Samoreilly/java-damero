# Kafka Damero

A Spring Boot library that adds automatic retry logic, dead letter queue handling, and circuit breaker support to Kafka listeners. The library reduces boilerplate code and provides production-ready error handling with minimal configuration.

## Status

Beta release. All core features are implemented and tested. The library is functional and ready for testing in development environments. Use in production at your own discretion.

## Features

- automatic retry logic with configurable backoff strategies (exponential, linear, fibonacci, custom)
- dead letter queue routing with complete metadata (attempts, timestamps, exceptions)
- distributed tracing with opentelemetry for full visibility into message processing
- circuit breaker integration using resilience4j
- rate limiting to control message processing throughput
- message deduplication to prevent duplicate processing
- conditional dlq routing to send different exceptions to different topics
- rest api for querying and replaying dlq messages
- distributed cache support using redis for multi-instance deployments
- metrics tracking with micrometer
- auto-configuration with sensible defaults

## Requirements

- Java 21 or higher
- Spring Boot 3.x
- Spring Kafka
- Apache Kafka

## Installation

add the library to your project:

```xml
<dependency>
    <groupid>java.damero</groupid>
    <artifactid>kafka-damero</artifactid>
    <version>0.1.0-snapshot</version>
</dependency>
```

the library automatically includes these dependencies:

- spring-boot-starter-aop
- micrometer-core
- resilience4j-circuitbreaker
- caffeine
- reflections
- commons-io

optional: add redis for distributed caching across multiple instances:

```xml
<dependency>
    <groupid>org.springframework.boot</groupid>
    <artifactid>spring-boot-starter-data-redis</artifactid>
</dependency>
```

optional: add opentelemetry for distributed tracing (recommended):

```xml
<dependency>
    <groupid>io.opentelemetry</groupid>
    <artifactid>opentelemetry-sdk</artifactid>
    <version>1.33.0</version>
</dependency>

<dependency>
    <groupid>io.opentelemetry</groupid>
    <artifactid>opentelemetry-exporter-otlp</artifactid>
    <version>1.33.0</version>
</dependency>
```

note: the example-app folder includes a fully configured working example with tracing already set up. copy the opentelemetryconfig class from there to get started quickly.

configure kafka in your application.properties:

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
@service
public class orderlistener {
    
    @customkafkalistener(
        topic = "orders",
        dlqtopic = "orders-dlq",
        maxattempts = 3,
        delay = 1000,
        delaymethod = delaymethod.expo,
        opentelemetry = true  // optional: enable distributed tracing
    )
    @kafkalistener(
        topics = "orders",
        groupid = "order-processor",
        containerfactory = "kafkalistenercontainerfactory"
    )
    public void processorder(consumerrecord<string, object> record, acknowledgment ack) {
        orderevent order = (orderevent) record.value();
        
        // process the order
        processpayment(order);
        
        ack.acknowledge();
    }
}
```

the library handles retries, dlq routing, and acknowledgment automatically. if you enable tracing (opentelemetry = true), you also get full visibility into message processing with zero additional code.

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
| messagesPerWindow | int | 0 | rate limit: messages per window (0 = disabled) |
| messageWindow | long | 0 | rate limit: window duration in ms |
| deDuplication | boolean | false | enable message deduplication |
| openTelemetry | boolean | false | enable distributed tracing |

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

### Distributed Tracing with OpenTelemetry

the library has built-in distributed tracing that lets you see exactly what happens to each message. this includes retries, dlq routing, circuit breaker events, deduplication checks, and rate limiting. the best part is that the library handles all the tracing automatically. you just need to add a simple configuration class.

#### why use tracing

tracing gives you complete visibility into your kafka message processing:

- see the full journey of each message from consumption to success or dlq
- track how many times a message was retried and why
- identify slow operations and bottlenecks
- correlate errors across multiple services
- monitor circuit breaker behavior and deduplication effectiveness
- debug production issues faster with detailed context

#### how easy is it to setup

the library does all the hard work for you. you only need to:

1. add opentelemetry dependencies to your pom.xml
2. create one simple configuration class (opentelemetryconfig)
3. enable tracing in your listener annotation

that is it. the library automatically creates spans for all operations and propagates trace context through kafka headers.

#### step 1: add dependencies

add these to your pom.xml:

```xml
<dependency>
    <groupId>io.opentelemetry</groupId>
    <artifactId>opentelemetry-sdk</artifactId>
    <version>1.33.0</version>
</dependency>

<dependency>
    <groupId>io.opentelemetry</groupId>
    <artifactId>opentelemetry-exporter-otlp</artifactId>
    <version>1.33.0</version>
</dependency>

<dependency>
    <groupId>io.opentelemetry.semconv</groupId>
    <artifactId>opentelemetry-semconv</artifactId>
    <version>1.23.1-alpha</version>
</dependency>
```

#### step 2: create opentelemetryconfig class

create this simple configuration class. the library will automatically use it:

```java
@Configuration
@ConditionalOnProperty(
    name = "otel.tracing.enabled",
    havingValue = "true",
    matchIfMissing = false
)
public class OpenTelemetryConfig {

    @value("${otel.exporter.otlp.endpoint:http://localhost:4317}")
    private string otlpendpoint;

    @value("${otel.service.name:my-service}")
    private string servicename;

    @bean
    public OpenTelemetry openTelemetry() {
        resource resource = resource.getDefault()
            .merge(resource.create(attributes.of(
                resourceattributes.service_name, servicename,
                resourceattributes.service_version, "1.0.0"
            )));

        otlpgrpcspanexporter spanexporter = otlpgrpcspanexporter.builder()
            .setendpoint(otlpendpoint)
            .build();

        sdktracerprovider sdktracerprovider = sdktracerprovider.builder()
            .addspanprocessor(batchspanprocessor.builder(spanexporter).build())
            .setresource(resource)
            .build();

        return OpenTelemetrysdk.builder()
            .settracerprovider(sdktracerprovider)
            .setpropagators(contextpropagators.create(
                w3ctracecontextpropagator.getinstance()
            ))
            .buildandregisterglobal();
    }
}
```

#### step 3: configure in application.properties

```properties
# enable tracing
otel.tracing.enabled=true

# your service name in traces
otel.service.name=my-kafka-service

# where to send traces (jaeger, tempo, cloud provider, etc)
otel.exporter.otlp.endpoint=http://localhost:4317
```

#### step 4: enable in your listener

```java
@customkafkalistener(
    topic = "orders",
    dlqtopic = "orders-dlq",
    maxattempts = 3,
    opentelemetry = true  // just add this
)
@kafkalistener(topics = "orders")
public void processorder(ConsumerRecord<String, Order> record) {
    // your code
}
```

#### step 5: start a tracing backend and view traces

for local development, start jaeger:

```bash
docker run -d --name jaeger -e collector_otlp_enabled=true \
  -p 16686:16686 -p 4317:4317 jaegertracing/all-in-one:latest
```

then view traces at http://localhost:16686

#### what gets traced automatically

the library automatically creates trace spans for:

- message processing (with success or failure status)
- retry scheduling and execution (with attempt numbers and delays)
- dlq routing (with reason and exception details)
- circuit breaker state changes (open, closed, half-open)
- deduplication checks (duplicate detected or first-time message)
- rate limiting operations (throttled or not)
- conditional dlq routing (which route was matched)

each span includes rich attributes like topic name, event id, retry attempt number, exception type, and more.

#### works with any tracing backend

because the library uses otlp (opentelemetry protocol), it works with any compatible backend:

- jaeger (local development)
- grafana tempo
- aws x-ray
- google cloud trace
- azure monitor
- datadog
- new relic
- honeycomb
- lightstep

you only need to change the endpoint in application.properties. the library code stays the same.

#### production configuration

for production, add sampling to reduce overhead:

```java
@value("${otel.traces.sampler.ratio:0.01}")  // sample 1% of traces
private double samplerratio;

sdktracerprovider sdkTracerProvider = sdkTracerProvider.builder()
    .setsampler(sampler.traceidratiobased(samplerratio))  // add this
    .addspanprocessor(...)
    .build();
```

```properties
# sample only 1% of traces in production
otel.traces.sampler.ratio=0.01

# sample 10% in staging
otel.traces.sampler.ratio=0.1

# sample 100% in development
otel.traces.sampler.ratio=1.0
```

#### example trace flow

when you send a message that fails and retries, you will see:

```
trace: a1b2c3d4e5f6...

damero.kafka.process [450ms] failed
  - messaging.destination: orders
  - damero.event_id: order-123
  - damero.outcome: retry_scheduled
  - damero.exception.type: timeoutexception

  damero.rate_limit [0ms] success
    - damero.rate_limit.throttled: false

  damero.deduplication.check [2ms] success
    - damero.deduplication.is_duplicate: false

  damero.retry [5ms] success
    - damero.retry.attempt: 1
    - damero.retry.delay_ms: 1000

(after retry delay...)

damero.kafka.process [350ms] failed
  - damero.retry.attempt: 2
  
  damero.retry [5ms] success
    - damero.retry.attempt: 2

(after retry delay...)

damero.kafka.process [400ms] failed
  - damero.retry.attempt: 3
  
  damero.dlq.route [25ms] success
    - damero.dlq.reason: max_attempts_reached
    - damero.dlq.attempts: 3
```

all spans share the same trace id so you can see the complete story.

#### see more

for detailed setup instructions and backend examples, see:
- viewing_traces.md - complete guide with all backends
- tracing_implementation.md - technical details
- backend_compatibility.md - examples for jaeger, tempo, aws, gcp, azure, etc.

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
public void processorder(consumerrecord<string, object> record, acknowledgment ack) {
    // process message
    ack.acknowledge();
}
```

the library handles acknowledgment timing to prevent duplicate processing.

### Distributed Tracing

enable tracing in development to understand message flow:

```java
@Customkafkalistener(
    topic = "orders",
    opentelemetry = true  // see exactly what happens to each message
)
```

in production, use sampling to reduce overhead:

```properties
otel.traces.sampler.ratio=0.01  # trace 1% of messages
```

tracing helps you debug issues faster by showing the complete journey of failed messages including all retry attempts and the final outcome.

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
