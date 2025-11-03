# Kafka Example Application

This is a standalone Spring Boot application to test the `kafka-damero` library.

## Prerequisites

1. **Java 21+**
2. **Maven 3.6+**
3. **Kafka running** (localhost:9092) or Docker:
   ```bash
   docker run -d --name kafka -p 9092:9092 apache/kafka:latest
   ```

## Setup

### 1. Build the Library First

From the root directory (`java-damero`):

```bash
mvn clean install
```

This installs the `kafka-damero` library to your local Maven repository.

### 2. Run the Example Application

From the `example-app` directory:

```bash
cd example-app
mvn spring-boot:run
```

The application will start on `http://localhost:8080`

## Testing Non-Retryable Exceptions

### Test 1: IllegalArgumentException (Non-Retryable)

```bash
curl -X POST http://localhost:8080/api/test/order/non-retryable/illegal-argument
```

**Expected Behavior:**
- Order sent with negative amount
- `IllegalArgumentException` thrown
- Message goes directly to DLQ with `attempts = 1`
- Check logs: `"exception IllegalArgumentException is non-retryable"`

### Test 2: ValidationException (Non-Retryable)

```bash
curl -X POST http://localhost:8080/api/test/order/non-retryable/validation
```

**Expected Behavior:**
- Order sent without customer ID
- `ValidationException` thrown
- Message goes directly to DLQ with `attempts = 1`

## Testing Retryable Exceptions

### Test 3: PaymentException (Retryable)

```bash
curl -X POST http://localhost:8080/api/test/order/retryable/payment
```

**Expected Behavior:**
- Order sent without payment method
- `PaymentException` thrown (not in nonRetryableExceptions)
- Message retried 3 times
- After 3 attempts, goes to DLQ with `attempts = 3`
- Check logs for retry attempts

### Test 4: RuntimeException (Retryable)

```bash
curl -X POST http://localhost:8080/api/test/order/retryable/runtime
```

**Expected Behavior:**
- Order sent with status "FAIL"
- `RuntimeException` thrown
- Message retried 3 times before going to DLQ

## Testing Success Case

```bash
curl -X POST http://localhost:8080/api/test/order/success
```

**Expected Behavior:**
- Valid order processed successfully
- No exceptions thrown
- Message acknowledged

## Testing Custom Order

```bash
curl -X POST http://localhost:8080/api/test/order/custom \
  -H "Content-Type: application/json" \
  -d '{
    "orderId": "order-999",
    "customerId": "customer-123",
    "amount": 150.0,
    "paymentMethod": "paypal",
    "status": "PENDING"
  }'
```

## Monitoring DLQ

The `DLQMonitor` service automatically logs all messages that end up in the DLQ:

```
=== DLQ Message Received ===
exception: IllegalArgumentException
attempts: 1
original topic: orders
dlq topic: orders-dlq
...
===========================
```

## Checking Kafka Topics

### List Topics
```bash
kafka-topics.sh --bootstrap-server localhost:9092 --list
```

### Consume from Main Topic
```bash
kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic orders \
  --from-beginning
```

### Consume from DLQ Topic
```bash
kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic orders-dlq \
  --from-beginning \
  --property print.key=true \
  --property print.value=true
```

## Configuration

The library is configured in `OrderProcessingService.java`:

```java
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
```

## Understanding the Results

- **attempts = 1**: Exception was non-retryable, went directly to DLQ
- **attempts = 3**: Exception was retryable, exhausted retries before going to DLQ
- **No DLQ message**: Order processed successfully

## Troubleshooting

1. **Kafka not running**: Make sure Kafka is running on localhost:9092
2. **Library not found**: Run `mvn install` from the root directory first
3. **Topics not created**: Kafka will auto-create topics, or create them manually:
   ```bash
   kafka-topics.sh --bootstrap-server localhost:9092 --create --topic orders
   kafka-topics.sh --bootstrap-server localhost:9092 --create --topic orders-dlq
   ```

