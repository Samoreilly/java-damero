# Minimal Setup Guide for kafka-damero

## ✨ The Beauty of Simplicity

Unlike other Kafka libraries that force you to manually manage a dozen dependencies, **kafka-damero handles everything for you**.

## 📦 What You Need (That's It!)

### Maven Dependencies

```xml
<dependencies>
    <!-- Basic Spring Boot -->
    <dependency>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-starter</artifactId>
    </dependency>
    
    <!-- Spring Kafka -->
    <dependency>
        <groupId>org.springframework.kafka</groupId>
        <artifactId>spring-kafka</artifactId>
    </dependency>
    
    <!-- The kafka-damero library -->
    <dependency>
        <groupId>java.damero</groupId>
        <artifactId>kafka-damero</artifactId>
        <version>0.1.0-SNAPSHOT</version>
    </dependency>
    
    <!-- Optional: If you want REST endpoints -->
    <dependency>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-starter-web</artifactId>
    </dependency>
    
    <!-- Optional: Lombok for your code -->
    <dependency>
        <groupId>org.projectlombok</groupId>
        <artifactId>lombok</artifactId>
        <optional>true</optional>
    </dependency>
</dependencies>
```

### Gradle Dependencies

```gradle
dependencies {
    implementation 'org.springframework.boot:spring-boot-starter'
    implementation 'org.springframework.kafka:spring-kafka'
    implementation 'java.damero:kafka-damero:0.1.0-SNAPSHOT'
    
    // Optional
    implementation 'org.springframework.boot:spring-boot-starter-web'
    compileOnly 'org.projectlombok:lombok'
}
```

## 🎁 What You Get Automatically

When you add `kafka-damero`, you automatically get:

✅ **Caffeine Cache** - For deduplication  
✅ **Resilience4j Circuit Breaker** - For fault tolerance  
✅ **Micrometer Metrics** - For monitoring  
✅ **Jackson JSON** - For serialization  
✅ **Spring AOP** - For annotations  
✅ **All transitive dependencies** - Properly managed

## ❌ What You DON'T Need to Add

You don't need to manually add:

- `com.github.ben-manes.caffeine:caffeine`
- `io.github.resilience4j:resilience4j-circuitbreaker`
- `io.micrometer:micrometer-core`
- `com.fasterxml.jackson.core:jackson-databind`
- `org.springframework.boot:spring-boot-starter-aop`
- `org.reflections:reflections`
- `commons-io:commons-io`

**The library handles all of this!**

## 🚀 Quick Start

### 1. Add the dependency (see above)

### 2. Configure in application.properties

```properties
# Kafka basics
spring.kafka.bootstrap-servers=localhost:9092
spring.kafka.consumer.group-id=my-consumer-group

# Enable kafka-damero features (optional - features are enabled by default)
custom.kafka.retry.enabled=true
custom.kafka.circuit-breaker.enabled=true
custom.kafka.deduplication.enabled=true
custom.kafka.rate-limiting.enabled=true
```

### 3. Use the annotations in your code

```java
@Service
public class OrderService {
    
    @CustomKafkaListener(
        topic = "orders",
        retryAttempts = 3,
        enableCircuitBreaker = true,
        enableDeduplication = true
    )
    public void processOrder(OrderEvent order) {
        // Your business logic
    }
}
```

That's it! No complex setup, no dependency hell, just simple, powerful Kafka processing.

## 📊 Compare with Other Solutions

### Other Libraries
```xml
<dependencies>
    <!-- Spring Kafka -->
    <dependency>...</dependency>
    
    <!-- Retry library -->
    <dependency>...</dependency>
    
    <!-- Circuit breaker -->
    <dependency>...</dependency>
    
    <!-- Metrics -->
    <dependency>...</dependency>
    
    <!-- Cache -->
    <dependency>...</dependency>
    
    <!-- JSON -->
    <dependency>...</dependency>
    
    <!-- AOP -->
    <dependency>...</dependency>
    
    <!-- ... 5 more dependencies -->
</dependencies>
```

### kafka-damero
```xml
<dependencies>
    <dependency>
        <groupId>org.springframework.kafka</groupId>
        <artifactId>spring-kafka</artifactId>
    </dependency>
    
    <dependency>
        <groupId>java.damero</groupId>
        <artifactId>kafka-damero</artifactId>
        <version>0.1.0-SNAPSHOT</version>
    </dependency>
</dependencies>
```

**One library. Zero headaches. Maximum productivity.**

