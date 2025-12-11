# Kafka-Damero Troubleshooting Guide

## Common Issues

### ❌ "No type information in headers and no default type provided"

**Problem**: Your Kafka consumer is receiving messages from producers that don't include Spring's `__TypeId__` header.

**Symptoms**:
```
java.lang.IllegalStateException: No type information in headers and no default type provided
    at org.springframework.kafka.support.serializer.JsonDeserializer.deserialize
```

**Root Cause**: 
- Messages sent from CLI tools (`kafka-console-producer`, `kafkacat`)
- Messages from non-Spring producers (Python, Go, Node.js)
- Messages from Spring producers NOT using `JsonSerializer` with `setAddTypeInfo(true)`

**Solutions** (choose ONE):

#### Option 1: Specify Default Type (Recommended for single event type)
```properties
# application.properties
kafka.damero.consumer.default-type=com.example.yourapp.model.OrderEvent
```

#### Option 2: Disable Type Headers (For non-Spring producers)
```properties
kafka.damero.consumer.use-type-headers=false
kafka.damero.consumer.default-type=com.example.yourapp.model.OrderEvent
```

#### Option 3: Use Spring's JsonSerializer in Producer
```java
@Bean
public KafkaTemplate<String, Object> kafkaTemplate(ObjectMapper kafkaObjectMapper) {
    Map<String, Object> props = new HashMap<>();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

    DefaultKafkaProducerFactory<String, Object> factory = new DefaultKafkaProducerFactory<>(props);
    JsonSerializer<Object> serializer = new JsonSerializer<>(kafkaObjectMapper);
    serializer.setAddTypeInfo(true);  // ← THIS LINE ADDS TYPE HEADERS
    factory.setValueSerializer(serializer);

    return new KafkaTemplate<>(factory);
}
```

---

### ⚠️ Multiple Listener Container Factories Conflict

**Problem**: You defined your own `ConcurrentKafkaListenerContainerFactory` but kafka-damero's auto-config is interfering.

**Solution**: Disable kafka-damero's container factory:
```properties
custom.kafka.auto-config.enabled=false
```

Then manually wire the components you need:
```java
@Configuration
public class MyKafkaConfig {
    @Bean
    public KafkaListenerAspect kafkaListenerAspect(...) {
        // Wire only the components you want from kafka-damero
    }
}
```

---

### 🔴 Redis Warnings in Logs (But You Don't Use Redis)

**Problem**: You see warnings like:
```
Redis not available - PluggableRedisCache using Caffeine in-memory cache
```

**Explanation**: This is **normal** for single-instance deployments. Kafka-damero falls back to Caffeine (in-memory cache).

**Action Required**: None, unless you're running multiple instances (then add Redis).

**To Silence Warning**: Add this dependency:
```xml
<dependency>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-starter-data-redis</artifactId>
</dependency>
```

And configure Redis in `application.properties`:
```properties
spring.data.redis.host=localhost
spring.data.redis.port=6379
```

---

### 📊 OpenTelemetry Tracing Not Working

**Problem**: No traces appearing in Jaeger/Zipkin.

**Solution**: Add OpenTelemetry dependencies:
```xml
<dependency>
    <groupId>io.opentelemetry</groupId>
    <artifactId>opentelemetry-api</artifactId>
    <version>1.32.0</version>
</dependency>
<dependency>
    <groupId>io.opentelemetry</groupId>
    <artifactId>opentelemetry-sdk</artifactId>
    <version>1.32.0</version>
</dependency>
<dependency>
    <groupId>io.opentelemetry</groupId>
    <artifactId>opentelemetry-exporter-otlp</artifactId>
    <version>1.32.0</version>
</dependency>
```

If OpenTelemetry is NOT on the classpath, kafka-damero uses `NoOpTracingService` (tracing disabled).

---

## Configuration Reference

### Required Properties
```properties
spring.kafka.bootstrap-servers=localhost:9092
spring.kafka.consumer.group-id=your-app-group
```

### Optional Properties
```properties
# Type header handling (default: true)
kafka.damero.consumer.use-type-headers=true

# Default type when headers missing (REQUIRED if use-type-headers=false)
kafka.damero.consumer.default-type=com.example.Event

# Disable auto-configuration (default: true)
custom.kafka.auto-config.enabled=true

# Redis strict mode (default: true)
damero.cache.strict-mode=true

# Deduplication TTL (default: 300000ms = 5 minutes)
kafka.damero.deduplication.ttl=300000
```

---

## Testing with CLI Tools

### Kafka Console Producer (No Type Headers)
```bash
# This WILL FAIL unless you configure default-type
kafka-console-producer --bootstrap-server localhost:9092 --topic orders \
  --property "parse.key=true" \
  --property "key.separator=:"

# Paste JSON:
order-123:{"orderId":"123","amount":99.99}
```

**Fix**: Add to `application.properties`:
```properties
kafka.damero.consumer.use-type-headers=false
kafka.damero.consumer.default-type=com.example.OrderEvent
```

---

## Getting Help

1. Check startup logs for `==> CONFIGURATION WARNING` messages
2. Enable debug logging:
   ```properties
   logging.level.net.damero=DEBUG
   ```
3. Open an issue with:
   - Full stack trace
   - `application.properties` (redact secrets)
   - Producer code snippet

