package net.damero.Kafka.Tracing;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import org.springframework.lang.Nullable;

/**
 * OpenTelemetry implementation of TracingContext that wraps a real OTel Context.
 * 
 * This class is only loaded when OpenTelemetry is on the classpath.
 */
public class OpenTelemetryTracingContext implements TracingContext {

    private final Context context;

    public OpenTelemetryTracingContext(Context context) {
        this.context = context;
    }

    @Override
    public TracingScope with(TracingSpan span) {
        if (span instanceof OpenTelemetryTracingSpan otelSpan) {
            Scope scope = context.with(otelSpan.getSpan()).makeCurrent();
            return new OpenTelemetryTracingScope(scope);
        }
        // Fallback for non-OTel spans (shouldn't happen in practice)
        return NoOpTracingScope.INSTANCE;
    }

    @Override
    @Nullable
    public Object getUnderlyingContext() {
        return context;
    }

    /**
     * Gets the underlying OpenTelemetry Context.
     * This is package-private for use by OpenTelemetryTracingService.
     */
    Context getContext() {
        return context;
    }
}

