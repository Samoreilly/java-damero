package net.damero.Kafka.Tracing;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import org.springframework.lang.Nullable;

/**
 * OpenTelemetry implementation of TracingSpan that wraps a real OTel Span.
 * 
 * This class is only loaded when OpenTelemetry is on the classpath.
 * It provides the actual tracing functionality by delegating to the underlying Span.
 */
public class OpenTelemetryTracingSpan implements TracingSpan {

    private final Span span;

    public OpenTelemetryTracingSpan(Span span) {
        this.span = span;
    }

    @Override
    public TracingSpan setAttribute(String key, String value) {
        span.setAttribute(key, value);
        return this;
    }

    @Override
    public TracingSpan setAttribute(String key, long value) {
        span.setAttribute(key, value);
        return this;
    }

    @Override
    public TracingSpan setAttribute(String key, boolean value) {
        span.setAttribute(key, value);
        return this;
    }

    @Override
    public TracingSpan setAttribute(String key, int value) {
        span.setAttribute(key, (long) value);
        return this;
    }

    @Override
    public TracingSpan recordException(Throwable exception) {
        span.recordException(exception);
        span.setStatus(StatusCode.ERROR, exception.getMessage());
        return this;
    }

    @Override
    public TracingSpan setSuccess() {
        span.setStatus(StatusCode.OK);
        return this;
    }

    @Override
    public TracingSpan setError(String message) {
        span.setStatus(StatusCode.ERROR, message);
        return this;
    }

    @Override
    public void end() {
        span.end();
    }

    @Override
    public TracingScope makeCurrent() {
        Scope scope = span.makeCurrent();
        return new OpenTelemetryTracingScope(scope);
    }

    @Override
    @Nullable
    public Object getUnderlyingSpan() {
        return span;
    }

    /**
     * Gets the underlying OpenTelemetry Span.
     * This is package-private for use by OpenTelemetryTracingService.
     */
    Span getSpan() {
        return span;
    }
}

