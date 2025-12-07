package net.damero.Kafka.Tracing;

import io.opentelemetry.context.Scope;

/**
 * OpenTelemetry implementation of TracingScope that wraps a real OTel Scope.
 *
 * This class is only loaded when OpenTelemetry is on the classpath.
 */
public class OpenTelemetryTracingScope implements TracingScope {

    private final Scope scope;

    public OpenTelemetryTracingScope(Scope scope) {
        this.scope = scope;
    }

    @Override
    public void close() {
        scope.close();
    }
}

