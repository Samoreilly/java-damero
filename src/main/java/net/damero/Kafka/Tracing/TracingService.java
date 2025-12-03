package net.damero.Kafka.Tracing;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.propagation.TextMapGetter;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 * Centralized service for managing OpenTelemetry tracing across the Damero library.
 * Provides utilities for creating, managing, and propagating trace spans.
 */
@Component
public class TracingService {
    
    private static final Logger logger = LoggerFactory.getLogger(TracingService.class);
    private static final String INSTRUMENTATION_NAME = "net.damero.kafka";
    
    private final Tracer tracer;
    
    public TracingService() {
        this.tracer = GlobalOpenTelemetry.getTracer(INSTRUMENTATION_NAME, "0.1.0");
        logger.debug("TracingService initialized with instrumentation name: {}", INSTRUMENTATION_NAME);
    }
    
    /**
     * Creates a new span for message processing.
     * 
     * @param operationName the name of the operation
     * @param topic the Kafka topic
     * @param eventId the event ID (optional)
     * @return a new Span instance
     */
    public Span startProcessingSpan(String operationName, String topic, @Nullable String eventId) {
        Span span = tracer.spanBuilder(operationName)
            .setSpanKind(SpanKind.CONSUMER)
            .startSpan();
        
        span.setAttribute("messaging.system", "kafka");
        span.setAttribute("messaging.destination", topic);
        span.setAttribute("messaging.operation", "process");
        span.setAttribute("damero.component", "kafka-listener");
        
        if (eventId != null) {
            span.setAttribute("damero.event_id", eventId);
        }
        
        return span;
    }
    
    /**
     * Creates a new span for retry operations.
     * 
     * @param topic the Kafka topic
     * @param eventId the event ID (optional)
     * @param attempt the current retry attempt
     * @param delayMs the retry delay in milliseconds
     * @return a new Span instance
     */
    public Span startRetrySpan(String topic, @Nullable String eventId, int attempt, long delayMs) {
        Span span = tracer.spanBuilder("damero.retry")
            .setSpanKind(SpanKind.INTERNAL)
            .startSpan();
        
        span.setAttribute("messaging.system", "kafka");
        span.setAttribute("messaging.destination", topic);
        span.setAttribute("damero.component", "retry-orchestrator");
        span.setAttribute("damero.retry.attempt", attempt);
        span.setAttribute("damero.retry.delay_ms", delayMs);
        
        if (eventId != null) {
            span.setAttribute("damero.event_id", eventId);
        }
        
        return span;
    }
    
    /**
     * Creates a new span for DLQ routing operations.
     * 
     * @param originalTopic the original Kafka topic
     * @param dlqTopic the DLQ topic
     * @param eventId the event ID (optional)
     * @param attempts the number of attempts made
     * @param reason the reason for DLQ routing
     * @return a new Span instance
     */
    public Span startDLQSpan(String originalTopic, String dlqTopic, @Nullable String eventId, 
                             int attempts, String reason) {
        Span span = tracer.spanBuilder("damero.dlq.route")
            .setSpanKind(SpanKind.PRODUCER)
            .startSpan();
        
        span.setAttribute("messaging.system", "kafka");
        span.setAttribute("messaging.source_destination", originalTopic);
        span.setAttribute("messaging.destination", dlqTopic);
        span.setAttribute("damero.component", "dlq-router");
        span.setAttribute("damero.dlq.attempts", attempts);
        span.setAttribute("damero.dlq.reason", reason);
        
        if (eventId != null) {
            span.setAttribute("damero.event_id", eventId);
        }
        
        return span;
    }
    
    /**
     * Creates a new span for circuit breaker operations.
     * 
     * @param topic the Kafka topic
     * @param state the circuit breaker state
     * @return a new Span instance
     */
    public Span startCircuitBreakerSpan(String topic, String state) {
        Span span = tracer.spanBuilder("damero.circuit_breaker." + state.toLowerCase())
            .setSpanKind(SpanKind.INTERNAL)
            .startSpan();
        
        span.setAttribute("messaging.system", "kafka");
        span.setAttribute("messaging.destination", topic);
        span.setAttribute("damero.component", "circuit-breaker");
        span.setAttribute("damero.circuit_breaker.state", state);
        
        return span;
    }
    
    /**
     * Creates a new span for deduplication checks.
     * 
     * @param eventId the event ID
     * @param isDuplicate whether the message is a duplicate
     * @return a new Span instance
     */
    public Span startDeduplicationSpan(String eventId, boolean isDuplicate) {
        Span span = tracer.spanBuilder("damero.deduplication.check")
            .setSpanKind(SpanKind.INTERNAL)
            .startSpan();
        
        span.setAttribute("damero.component", "deduplication-manager");
        span.setAttribute("damero.event_id", eventId);
        span.setAttribute("damero.deduplication.is_duplicate", isDuplicate);
        
        return span;
    }
    
    /**
     * Creates a new span for rate limiting operations.
     * 
     * @param topic the Kafka topic
     * @param currentCount the current message count in window
     * @param maxMessages the maximum messages allowed per window
     * @param sleepMs the sleep time in milliseconds (0 if not rate limited)
     * @return a new Span instance
     */
    public Span startRateLimitSpan(String topic, int currentCount, int maxMessages, long sleepMs) {
        Span span = tracer.spanBuilder("damero.rate_limit")
            .setSpanKind(SpanKind.INTERNAL)
            .startSpan();
        
        span.setAttribute("messaging.system", "kafka");
        span.setAttribute("messaging.destination", topic);
        span.setAttribute("damero.component", "rate-limiter");
        span.setAttribute("damero.rate_limit.current_count", currentCount);
        span.setAttribute("damero.rate_limit.max_messages", maxMessages);
        span.setAttribute("damero.rate_limit.sleep_ms", sleepMs);
        span.setAttribute("damero.rate_limit.throttled", sleepMs > 0);
        
        return span;
    }
    
    /**
     * Creates a new span for DLQ replay operations.
     * 
     * @param dlqTopic the DLQ topic being replayed
     * @return a new Span instance
     */
    public Span startReplaySpan(String dlqTopic) {
        Span span = tracer.spanBuilder("damero.dlq.replay")
            .setSpanKind(SpanKind.CONSUMER)
            .startSpan();
        
        span.setAttribute("messaging.system", "kafka");
        span.setAttribute("messaging.source_destination", dlqTopic);
        span.setAttribute("damero.component", "dlq-replay");
        
        return span;
    }
    
    /**
     * Records an exception in the current span.
     * 
     * @param span the span to record the exception in
     * @param exception the exception to record
     */
    public void recordException(Span span, Throwable exception) {
        if (span != null) {
            span.recordException(exception);
            span.setStatus(StatusCode.ERROR, exception.getMessage());
        }
    }
    
    /**
     * Sets the span status to OK.
     * 
     * @param span the span to set status on
     */
    public void setSuccess(Span span) {
        if (span != null) {
            span.setStatus(StatusCode.OK);
        }
    }
    
    /**
     * Ends a span safely.
     * 
     * @param span the span to end
     */
    public void endSpan(@Nullable Span span) {

        if (span != null) {
            span.end();
        }
    }
    
    /**
     * Extracts the trace context from Kafka headers.
     * 
     * @param headers the Kafka headers
     * @return the extracted Context
     */
    public Context extractContext(Headers headers) {
        if (headers == null) {
            return Context.current();
        }
        
        Map<String, String> headerMap = new HashMap<>();
        headers.forEach(header -> {
            if (header.key().startsWith("traceparent") || 
                header.key().startsWith("tracestate") ||
                header.key().startsWith("baggage")) {
                headerMap.put(header.key(), new String(header.value(), StandardCharsets.UTF_8));
            }
        });
        
        return GlobalOpenTelemetry.getPropagators()
            .getTextMapPropagator()
            .extract(Context.current(), headerMap, new MapGetter());
    }
    
    /**
     * Injects the current trace context into Kafka headers.
     * 
     * @param headers the Kafka headers to inject into
     * @param context the context to inject (if null, uses current context)
     */
    public void injectContext(Headers headers, @Nullable Context context) {
        if (headers == null) {
            return;
        }
        
        Context ctx = context != null ? context : Context.current();
        
        GlobalOpenTelemetry.getPropagators()
            .getTextMapPropagator()
            .inject(ctx, headers, (hdrs, key, value) -> {
                if (hdrs != null) {
                    hdrs.add(new RecordHeader(key, value.getBytes(StandardCharsets.UTF_8)));
                }
            });
    }
    
    /**
     * Wraps a runnable with the current span context.
     * 
     * @param span the span to use as context
     * @param runnable the runnable to wrap
     * @return a wrapped runnable that runs in the span's context
     */
    public Runnable wrapWithContext(Span span, Runnable runnable) {
        Context context = Context.current().with(span);
        return context.wrap(runnable);
    }
    
    /**
     * Gets the current tracer instance.
     * 
     * @return the tracer
     */
    public Tracer getTracer() {
        return tracer;
    }
    
    /**
     * TextMapGetter implementation for extracting headers from a Map.
     */
    private static class MapGetter implements TextMapGetter<Map<String, String>> {
        @Override
        public Iterable<String> keys(Map<String, String> carrier) {
            return carrier.keySet();
        }
        
        @Override
        public String get(Map<String, String> carrier, String key) {
            return carrier.get(key);
        }
    }
}

