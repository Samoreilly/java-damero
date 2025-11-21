# DLQ Metrics API - Documentation


Your DLQ endpoint now has **3 different views** to suit different needs:

### 1. Raw format
**Endpoint:** `GET /dlq/raw?topic=test-dlq`

This is your original format

### 2. Enhanced format
**Endpoint:** `GET /dlq?topic=test-dlq`

Human-readable format with summary statistics - perfect for operations teams!

### 3. Stats only (quick overview)
**Endpoint:** `GET /dlq/stats?topic=test-dlq`


### Enhanced example:

```json
{
  "summary": {
    "totalEvents": 5,
    "highSeverityCount": 2,
    "mediumSeverityCount": 1,
    "lowSeverityCount": 2,
    "statusBreakdown": {
      "FAILED_MAX_RETRIES": 3,
      "NON_RETRYABLE_EXCEPTION": 2
    },
    "exceptionTypeBreakdown": {
      "RuntimeException": 3,
      "ValidationException": 1,
      "IllegalArgumentException": 1
    },
    "topicBreakdown": {
      "orders": 5
    },
    "averageAttempts": 2.4,
    "maxAttemptsObserved": 3,
    "minAttemptsObserved": 1,
    "oldestEvent": "2025-11-21 13:10:15",
    "newestEvent": "2025-11-21 13:12:01"
  },
  "events": [
    {
      "eventId": "order-004",
      "eventType": "OrderEvent",
      "status": "FAILED_MAX_RETRIES",
      "severity": "HIGH",
      
      "firstFailureTime": "2025-11-21 13:12:01",
      "lastFailureTime": "2025-11-21 13:12:01",
      "timeInDlq": "15 minutes 30 seconds",
      "failureDuration": "0 seconds",
      
      "totalAttempts": 3,
      "maxAttemptsConfigured": 3,
      "maxAttemptsReached": true,
      
      "originalTopic": "orders",
      "dlqTopic": "test-dlq",
      
      "firstException": {
        "type": "RuntimeException",
        "message": "simulated processing failure",
        "rootCause": null,
        "timestamp": "2025-11-21 13:12:01",
        "stackTracePreview": [
          "com.example.kafkaexample.service.OrderProcessingService.processOrder(OrderProcessingService.java:60)",
          "net.damero.Kafka.Aspect.KafkaListenerAspect.aroundKafkaListener(KafkaListenerAspect.java:45)",
          "java.base/jdk.internal.reflect.DirectMethodHandleAccessor.invoke(DirectMethodHandleAccessor.java:104)"
        ]
      },
      "lastException": {
        "type": "RuntimeException",
        "message": "simulated processing failure",
        "timestamp": "2025-11-21 13:12:01",
        "stackTracePreview": [
          "com.example.kafkaexample.service.OrderProcessingService.processOrder(OrderProcessingService.java:60)",
          "net.damero.Kafka.Aspect.KafkaListenerAspect.aroundKafkaListener(KafkaListenerAspect.java:45)",
          "java.base/jdk.internal.reflect.DirectMethodHandleAccessor.invoke(DirectMethodHandleAccessor.java:104)"
        ]
      },
      "sameExceptionType": true,
      
      "retryConfig": {
        "delayMs": 1000,
        "delayMethod": "FIBONACCI",
        "maxAttempts": 3
      },
      
      "originalEvent": {
        "orderId": "order-004",
        "customerId": "customer-123",
        "amount": 100.0,
        "paymentMethod": "credit-card",
        "status": "FAIL"
      }
    }
  ],
  "topic": "test-dlq",
  "pageSize": 5,
  "queriedAt": "2025-11-21 13:27:31"
}
```

## Dashboard integration

The enhanced format is **perfect** for monitoring dashboards:

```javascript
// Example: Show alert if high severity events exist
fetch('/dlq/stats?topic=production-dlq')
  .then(res => res.json())
  .then(stats => {
    if (stats.highSeverityCount > 0) {
      alert(`⚠️ ${stats.highSeverityCount} high severity failures!`);
    }
  });
```
