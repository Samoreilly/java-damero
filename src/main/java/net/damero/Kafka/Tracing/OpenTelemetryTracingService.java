package net.damero.Kafka.Tracing;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.propagation.TextMapGetter;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.lang.Nullable;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 * OpenTelemetry implementation of TracingService.
 * 
 * This class is only loaded when OpenTelemetry is on the classpath.
 * It provides real distributed tracing functionality using OpenTelemetry APIs.
 * 
 * IMPORTANT: This class imports OpenTelemetry classes directly. The JVM will only
 * attempt to load this class when it's actually instantiated, which only happens
 * when @ConditionalOnClass passes (i.e., OpenTelemetry is available).
 */
public class OpenTelemetryTracingService implements TracingService {

    private static final Logger logger = LoggerFactory.getLogger(OpenTelemetryTracingService.class);
    private static final String INSTRUMENTATION_NAME = "net.damero.kafka";

    private final Tracer tracer;

    public OpenTelemetryTracingService() {
        this.tracer = GlobalOpenTelemetry.getTracer(INSTRUMENTATION_NAME, "0.1.0");
        logger.info("OpenTelemetryTracingService initialized - distributed tracing enabled");
        logger.debug("Using instrumentation name: {}", INSTRUMENTATION_NAME);
    }

    @Override
    public TracingSpan startProcessingSpan(String operationName, String topic, @Nullable String eventId) {
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

        return new OpenTelemetryTracingSpan(span);
    }

    @Override
    public TracingSpan startRetrySpan(String topic, @Nullable String eventId, int attempt, long delayMs) {
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

        return new OpenTelemetryTracingSpan(span);
    }

    @Override
    public TracingSpan startDLQSpan(String originalTopic, String dlqTopic, @Nullable String eventId,
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

        return new OpenTelemetryTracingSpan(span);
    }

    @Override
    public TracingSpan startCircuitBreakerSpan(String topic, String state) {
        Span span = tracer.spanBuilder("damero.circuit_breaker." + state.toLowerCase())
            .setSpanKind(SpanKind.INTERNAL)
            .startSpan();

        span.setAttribute("messaging.system", "kafka");
        span.setAttribute("messaging.destination", topic);
        span.setAttribute("damero.component", "circuit-breaker");
        span.setAttribute("damero.circuit_breaker.state", state);

        return new OpenTelemetryTracingSpan(span);
    }

    @Override
    public TracingSpan startDeduplicationSpan(String eventId, boolean isDuplicate) {
        Span span = tracer.spanBuilder("damero.deduplication.check")
            .setSpanKind(SpanKind.INTERNAL)
            .startSpan();

        span.setAttribute("damero.component", "deduplication-manager");
        span.setAttribute("damero.event_id", eventId);
        span.setAttribute("damero.deduplication.is_duplicate", isDuplicate);

        return new OpenTelemetryTracingSpan(span);
    }

    @Override
    public TracingSpan startRateLimitSpan(String topic, int currentCount, int maxMessages, long sleepMs) {
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

        return new OpenTelemetryTracingSpan(span);
    }

    @Override
    public TracingSpan startReplaySpan(String dlqTopic) {
        Span span = tracer.spanBuilder("damero.dlq.replay")
            .setSpanKind(SpanKind.CONSUMER)
            .startSpan();

        span.setAttribute("messaging.system", "kafka");
        span.setAttribute("messaging.source_destination", dlqTopic);
        span.setAttribute("damero.component", "dlq-replay");

        return new OpenTelemetryTracingSpan(span);
    }

    @Override
    public TracingContext extractContext(Headers headers) {
        if (headers == null) {
            return new OpenTelemetryTracingContext(Context.current());
        }

        Map<String, String> headerMap = new HashMap<>();
        headers.forEach(header -> {
            if (header.key().startsWith("traceparent") ||
                header.key().startsWith("tracestate") ||
                header.key().startsWith("baggage")) {
                headerMap.put(header.key(), new String(header.value(), StandardCharsets.UTF_8));
            }
        });

        Context context = GlobalOpenTelemetry.getPropagators()
            .getTextMapPropagator()
            .extract(Context.current(), headerMap, new MapGetter());

        return new OpenTelemetryTracingContext(context);
    }

    @Override
    public void injectContext(Headers headers, @Nullable TracingContext context) {
        if (headers == null) {
            return;
        }

        Context ctx;
        if (context instanceof OpenTelemetryTracingContext otelContext) {
            ctx = otelContext.getContext();
        } else {
            ctx = Context.current();
        }

        GlobalOpenTelemetry.getPropagators()
            .getTextMapPropagator()
            .inject(ctx, headers, (hdrs, key, value) -> {
                if (hdrs != null) {
                    hdrs.add(new RecordHeader(key, value.getBytes(StandardCharsets.UTF_8)));
                }
            });
    }

    @Override
    public Runnable wrapWithContext(TracingSpan span, Runnable runnable) {
        if (span instanceof OpenTelemetryTracingSpan otelSpan) {
            Context context = Context.current().with(otelSpan.getSpan());
            return context.wrap(runnable);
        }
        return runnable;
    }

    @Override
    public boolean isEnabled() {
        return true;
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

