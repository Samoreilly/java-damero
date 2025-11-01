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
 * Circuit breaker is optional, if user doesnt provide Resilience4j, this service wont be created.
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
                //Get config class for circuit breaker
                Class<?> configClass = Class.forName("io.github.resilience4j.circuitbreaker.CircuitBreakerConfig");
                Method customMethod = configClass.getMethod("custom");
                Object configBuilder = customMethod.invoke(null);
                
                // Build config for circuit breaker
                Class<?> builderClass = configBuilder.getClass();
                builderClass.getMethod("failureRateThreshold", float.class).invoke(configBuilder, 50.0f);//threshold for opening the circuit
                builderClass.getMethod("slidingWindowSize", int.class).invoke(configBuilder, failureThreshold);//window size for tracking failures
                
                Class<?> slidingWindowTypeEnum = Class.forName("io.github.resilience4j.circuitbreaker.CircuitBreakerConfig$SlidingWindowType");
                Object countBased = Enum.valueOf((Class<Enum>) slidingWindowTypeEnum, "COUNT_BASED");
                builderClass.getMethod("slidingWindowType", slidingWindowTypeEnum).invoke(configBuilder, countBased);//type of window for tracking failures
                
                builderClass.getMethod("waitDurationInOpenState", Duration.class).invoke(configBuilder, Duration.ofMillis(waitDuration));//wait duration for half open state
                builderClass.getMethod("permittedNumberOfCallsInHalfOpenState", int.class).invoke(configBuilder, 3);//number of calls permitted in half open state
                builderClass.getMethod("minimumNumberOfCalls", int.class).invoke(configBuilder, failureThreshold);//minimum number of calls required to open the circuit
                

                Method buildMethod = builderClass.getMethod("build");
                Object config = buildMethod.invoke(configBuilder);
                
                Method circuitBreakerMethod = circuitBreakerRegistry.getClass().getMethod("circuitBreaker", String.class, configClass);
                return circuitBreakerMethod.invoke(circuitBreakerRegistry, topic, config);
            } catch (Exception e) {
                //return null if circuit breaker is not available
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

