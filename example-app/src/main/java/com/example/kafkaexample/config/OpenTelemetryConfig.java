package com.example.kafkaexample.config;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.exporter.otlp.trace.OtlpGrpcSpanExporter;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor;
import io.opentelemetry.sdk.trace.samplers.Sampler;
import io.opentelemetry.semconv.ResourceAttributes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;

/**
 * opentelemetry configuration for distributed tracing
 *
 * setup instructions:
 * 1. add to application.properties:
 *    otel.tracing.enabled=true
 *    otel.service.name=your-service-name
 *    otel.exporter.otlp.endpoint=http://localhost:4317
 *    otel.traces.sampler.ratio=0.1  (sample 10% in production, 1.0 for dev)
 *
 * 2. start jaeger locally (for development):
 *    docker run -d --name jaeger -e COLLECTOR_OTLP_ENABLED=true -p 16686:16686 -p 4317:4317 jaegertracing/all-in-one:latest
 *
 * 3. verify dependencies in pom.xml:
 *    - opentelemetry-sdk
 *    - opentelemetry-exporter-otlp
 *    - opentelemetry-semconv
 *
 * 4. enable tracing in kafka listeners:
 *    @CustomKafkaListener(topic = "my-topic", openTelemetry = true)
 *
 * 5. view traces at http://localhost:16686 (jaeger) or your configured backend
 *
 * production notes:
 * - set otel.traces.sampler.ratio to low value like 0.01 (1%) or 0.1 (10%)
 * - use environment-specific endpoints via spring profiles
 * - consider using spring cloud sleuth for automatic instrumentation
 * - monitor exporter health and handle failures gracefully
 */
@Configuration
@ConditionalOnProperty(
    name = "otel.tracing.enabled",
    havingValue = "true",
    matchIfMissing = false
)
public class OpenTelemetryConfig {

    private static final Logger logger = LoggerFactory.getLogger(OpenTelemetryConfig.class);

    //port 4317 is where traces are sent by default
    @Value("${otel.exporter.otlp.endpoint:http://localhost:4317}")
    private String otlpEndpoint;

    @Value("${otel.service.name:damero-service}")
    private String serviceName;

    @Value("${otel.service.version:1.0.0}")
    private String serviceVersion;

    @Value("${otel.traces.sampler.ratio:1.0}")
    private double samplerRatio;

    @Value("${otel.deployment.environment:development}")
    private String deploymentEnvironment;

    @Value("${otel.exporter.otlp.timeout:10000}")
    private long exporterTimeoutMs;

    @Bean
    public OpenTelemetry openTelemetry() {
        logger.info("initializing opentelemetry - endpoint: {}, service: {}, environment: {}, sampling: {}%",
            otlpEndpoint, serviceName, deploymentEnvironment, samplerRatio * 100);

        try {
            // define resource attributes to identify your service in traces
            Resource resource = Resource.getDefault()
                .merge(Resource.create(Attributes.of(
                    ResourceAttributes.SERVICE_NAME, serviceName,
                    ResourceAttributes.SERVICE_VERSION, serviceVersion,
                    ResourceAttributes.DEPLOYMENT_ENVIRONMENT, deploymentEnvironment
                )));

            // configure otlp exporter with timeout and error handling
            OtlpGrpcSpanExporter spanExporter = OtlpGrpcSpanExporter.builder()
                .setEndpoint(otlpEndpoint)
                .setTimeout(Duration.ofMillis(exporterTimeoutMs))
                .build();

            // configure sampling for production efficiency
            // ratio of 1.0 = 100% (development), 0.1 = 10%, 0.01 = 1% (production)
            Sampler sampler = Sampler.traceIdRatioBased(samplerRatio);

            // configure tracer provider with batch processing for efficiency
            SdkTracerProvider sdkTracerProvider = SdkTracerProvider.builder()
                .setSampler(sampler)
                .addSpanProcessor(BatchSpanProcessor.builder(spanExporter)
                    .setMaxExportBatchSize(512)
                    .setMaxQueueSize(2048)
                    .setExporterTimeout(Duration.ofMillis(exporterTimeoutMs))
                    .setScheduleDelay(Duration.ofMillis(5000))
                    .build())
                .setResource(resource)
                .build();

            // build and register global opentelemetry instance
            // the damero library automatically uses this via GlobalOpenTelemetry
            OpenTelemetry openTelemetry = OpenTelemetrySdk.builder()
                .setTracerProvider(sdkTracerProvider)
                .setPropagators(ContextPropagators.create(
                    W3CTraceContextPropagator.getInstance()
                ))
                .buildAndRegisterGlobal();

            // ensure proper shutdown to flush remaining spans
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                logger.info("shutting down opentelemetry...");
                try {
                    sdkTracerProvider.close();
                    logger.info("opentelemetry shutdown complete");
                } catch (Exception e) {
                    logger.error("error during opentelemetry shutdown: {}", e.getMessage(), e);
                }
            }));

            logger.info("opentelemetry initialized successfully");
            return openTelemetry;

        } catch (Exception e) {
            logger.error("failed to initialize opentelemetry: {}", e.getMessage(), e);
            logger.warn("tracing will be disabled - application will continue without traces");

            // return no-op implementation to prevent application crash
            return OpenTelemetry.noop();
        }
    }
}

