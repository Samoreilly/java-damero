# How Your Code Uses OpenTelemetryConfig

## Overview

Your `OpenTelemetryConfig` works through a **global registration pattern**. Here's the complete flow:

---

## The Flow

```
1. Spring Boot Startup
   ↓
2. OpenTelemetryConfig.openTelemetry() bean is created
   ↓
3. .buildAndRegisterGlobal() is called
   ↓
4. OpenTelemetry instance is registered GLOBALLY
   ↓
5. TracingService uses GlobalOpenTelemetry.getTracer()
   ↓
6. All tracing automatically uses your configuration
```

---

## Step-by-Step Breakdown

### Step 1: Spring Boot Creates the Bean

When your application starts, Spring Boot automatically calls:

```java
// In OpenTelemetryConfig.java
@Bean
public OpenTelemetry openTelemetry() {
    // ... configuration code ...
    
    OpenTelemetry openTelemetry = OpenTelemetrySdk.builder()
        .setTracerProvider(sdkTracerProvider)
        .setPropagators(...)
        .buildAndRegisterGlobal();  // ← KEY: Registers globally!
        
    return openTelemetry;
}
```

The **`.buildAndRegisterGlobal()`** method is critical - it registers your OpenTelemetry instance as the global default.

---

### Step 2: TracingService Gets the Tracer

In the Damero library, `TracingService` is initialized:

```java
// In TracingService.java
@Component
public class TracingService {
    private final Tracer tracer;
    
    public TracingService() {
        // Gets the globally registered OpenTelemetry instance
        this.tracer = GlobalOpenTelemetry.getTracer("net.damero.kafka", "0.1.0");
    }
}
```

**Key Point**: `GlobalOpenTelemetry.getTracer()` automatically uses the OpenTelemetry instance you registered in step 1.

---

### Step 3: All Components Use TracingService

The Damero library components receive `TracingService` via dependency injection:

```java
// In CustomKafkaAutoConfiguration.java
@Bean
public TracingService tracingService() {
    return new TracingService();  // Uses GlobalOpenTelemetry internally
}

@Bean
public DLQRouter dlqRouter(TracingService tracingService) {
    return new DLQRouter(tracingService);
}

@Bean
public KafkaListenerAspect kafkaListenerAspect(..., TracingService tracingService) {
    return new KafkaListenerAspect(..., tracingService);
}
```

---

### Step 4: Traces Flow to Your Configured Backend

When your code processes a Kafka message:

```java
// In KafkaListenerAspect.java
if (customKafkaListener.openTelemetry()) {
    Span processingSpan = tracingService.startProcessingSpan(...);
    // ↓
    // TracingService uses the Tracer
    // ↓
    // Tracer uses GlobalOpenTelemetry
    // ↓
    // GlobalOpenTelemetry uses YOUR OpenTelemetryConfig
    // ↓
    // Spans are exported to Jaeger (localhost:4317)
}
```

---

## The Connection Chain

```
Your OpenTelemetryConfig
        ↓
    .buildAndRegisterGlobal()
        ↓
    GlobalOpenTelemetry (Singleton)
        ↓
    TracingService.tracer
        ↓
    TracingService.startProcessingSpan()
        ↓
    Span created with your exporter
        ↓
    OtlpGrpcSpanExporter
        ↓
    Jaeger at localhost:4317
```

---

## Why This Design?

### 1. **No Direct Dependencies**
The Damero library never directly references your `OpenTelemetryConfig`. It only uses `GlobalOpenTelemetry`, which is a standard OpenTelemetry pattern.

### 2. **User Control**
You (the user) control:
- Where traces go (Jaeger, Zipkin, cloud providers)
- Service name and version
- Sampling rate
- Export batch size
- Any other OpenTelemetry configuration

### 3. **Spring Boot Auto-Configuration**
Since `OpenTelemetryConfig` is annotated with `@Configuration` and has a `@Bean` method, Spring Boot automatically:
- Detects it
- Creates the bean
- Calls `.buildAndRegisterGlobal()`
- Makes it available to all components

---

## What Happens at Runtime

### When a Kafka Message Arrives:

```
1. Message → KafkaListenerAspect
2. Check: openTelemetry = true?
3. YES → tracingService.startProcessingSpan()
4. TracingService creates span using its tracer
5. Tracer is connected to GlobalOpenTelemetry
6. GlobalOpenTelemetry uses YOUR config
7. Span data → OtlpGrpcSpanExporter
8. Exporter sends to localhost:4317
9. Jaeger receives and stores the trace
10. You view it at http://localhost:16686
```

---

## Configuration Properties

You can customize via `application.properties`:

```properties
# These are read by OpenTelemetryConfig
otel.exporter.otlp.endpoint=http://localhost:4317
otel.service.name=kafka-example-app
otel.service.version=1.0.0
otel.traces.sampler.ratio=1.0
```

In `OpenTelemetryConfig.java`:
```java
@Value("${otel.exporter.otlp.endpoint:http://localhost:4317}")
private String otlpEndpoint;  // ← Injected from properties
```

---

## Without OpenTelemetryConfig

If you **don't** create `OpenTelemetryConfig`:

```
GlobalOpenTelemetry.getTracer()
        ↓
    Returns a NO-OP tracer (does nothing)
        ↓
    Spans are created but discarded
        ↓
    No traces exported anywhere
```

This is safe - the library won't crash, but you won't see any traces.

---

## Multiple Services

If you have multiple services, each has its own `OpenTelemetryConfig`:

```
Service A (OpenTelemetryConfig)
    ↓
service.name = "order-service"
exporter → Jaeger

Service B (OpenTelemetryConfig)
    ↓
service.name = "payment-service"
exporter → Jaeger

Service C (OpenTelemetryConfig)
    ↓
service.name = "inventory-service"
exporter → Jaeger
```

All traces go to the same Jaeger instance, but are tagged with different service names. You can follow traces across all services!

---
