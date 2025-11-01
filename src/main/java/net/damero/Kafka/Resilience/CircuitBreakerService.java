package net.damero.Kafka.Resilience;

import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Component;

import java.lang.reflect.Method;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Service to manage circuit breakers per topic.
 * Circuit breakers are optional - if Resilience4j is not available, this service won't be created.
 * Uses reflection to avoid compile-time dependency on Resilience4j.
 */
@Component
@ConditionalOnClass(name = "io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry")
public class CircuitBreakerService {

    @Nullable
    private final Object circuitBreakerRegistry;

    private final Map<String, Object> circuitBreakers = new ConcurrentHashMap<>();

    public CircuitBreakerService(@Nullable Object circuitBreakerRegistry) {
        this.circuitBreakerRegistry = circuitBreakerRegistry;
    }

    /**
     * Get or create a circuit breaker for a topic with custom configuration.
     * 
     * @param topic The Kafka topic
     * @param failureThreshold Number of failures before opening circuit
     * @param windowDuration Time window in milliseconds to track failures
     * @param waitDuration Time to wait before transitioning from OPEN to HALF_OPEN (ms)
     * @return Circuit breaker instance, or null if Resilience4j not available
     */
    @Nullable
    public Object getCircuitBreaker(String topic, 
                                    int failureThreshold,
                                    long windowDuration,
                                    long waitDuration) {
        if (circuitBreakerRegistry == null) {
            return null;  // Resilience4j not available
        }

        return circuitBreakers.computeIfAbsent(topic, t -> {
            try {
                // Create CircuitBreakerConfig using reflection
                Class<?> configClass = Class.forName("io.github.resilience4j.circuitbreaker.CircuitBreakerConfig");
                Method customMethod = configClass.getMethod("custom");
                Object configBuilder = customMethod.invoke(null);
                
                // Configure using builder pattern via reflection
                Class<?> builderClass = configBuilder.getClass();
                builderClass.getMethod("failureRateThreshold", float.class).invoke(configBuilder, 50.0f);
                builderClass.getMethod("slidingWindowSize", int.class).invoke(configBuilder, failureThreshold);
                
                Class<?> slidingWindowTypeEnum = Class.forName("io.github.resilience4j.circuitbreaker.CircuitBreakerConfig$SlidingWindowType");
                Object countBased = Enum.valueOf((Class<Enum>) slidingWindowTypeEnum, "COUNT_BASED");
                builderClass.getMethod("slidingWindowType", slidingWindowTypeEnum).invoke(configBuilder, countBased);
                
                builderClass.getMethod("waitDurationInOpenState", Duration.class).invoke(configBuilder, Duration.ofMillis(waitDuration));
                builderClass.getMethod("permittedNumberOfCallsInHalfOpenState", int.class).invoke(configBuilder, 3);
                builderClass.getMethod("minimumNumberOfCalls", int.class).invoke(configBuilder, failureThreshold);
                
                // Build config
                Method buildMethod = builderClass.getMethod("build");
                Object config = buildMethod.invoke(configBuilder);
                
                // Create circuit breaker from registry
                Method circuitBreakerMethod = circuitBreakerRegistry.getClass().getMethod("circuitBreaker", String.class, configClass);
                return circuitBreakerMethod.invoke(circuitBreakerRegistry, topic, config);
            } catch (Exception e) {
                // If reflection fails, return null (circuit breaker not available)
                return null;
            }
        });
    }

    /**
     * Check if circuit breaker is available (Resilience4j on classpath).
     */
    public boolean isAvailable() {
        return circuitBreakerRegistry != null;
    }

    /**
     * Get circuit breaker state as string for logging.
     */
    public String getStateName(@Nullable Object circuitBreaker) {
        if (circuitBreaker == null) {
            return "N/A";
        }
        try {
            Method getStateMethod = circuitBreaker.getClass().getMethod("getState");
            Object state = getStateMethod.invoke(circuitBreaker);
            Method nameMethod = state.getClass().getMethod("name");
            return (String) nameMethod.invoke(state);
        } catch (Exception e) {
            return "UNKNOWN";
        }
    }
}

