package net.damero.Kafka.Aspect.Components;

import net.damero.Kafka.Resilience.CircuitBreakerService;
import net.damero.Kafka.Annotations.CustomKafkaListener;
import org.aspectj.lang.ProceedingJoinPoint;
import org.springframework.lang.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.function.Supplier;

/**
 * Wrapper for circuit breaker functionality using reflection to avoid compile-time dependency.
 */
public class CircuitBreakerWrapper {
    
    private static final Logger logger = LoggerFactory.getLogger(CircuitBreakerWrapper.class);
    
    @Nullable
    private final CircuitBreakerService circuitBreakerService;

    public CircuitBreakerWrapper(@Nullable CircuitBreakerService circuitBreakerService) {
        this.circuitBreakerService = circuitBreakerService;
    }

    /**
     * Gets a circuit breaker instance for the given listener configuration.
     * 
     * @param customKafkaListener the listener configuration
     * @return the circuit breaker instance, or null if not available
     */
    @Nullable
    public Object getCircuitBreaker(CustomKafkaListener customKafkaListener) {
        if (!customKafkaListener.enableCircuitBreaker() || 
            circuitBreakerService == null || 
            !circuitBreakerService.isAvailable()) {
            return null;
        }

        return circuitBreakerService.getCircuitBreaker(
            customKafkaListener.topic(),
            customKafkaListener.circuitBreakerFailureThreshold(),
            customKafkaListener.circuitBreakerWindowDuration(),
            customKafkaListener.circuitBreakerWaitDuration()
        );
    }

    /**
     * Checks if circuit breaker is OPEN.
     * 
     * @param circuitBreaker the circuit breaker instance
     * @return true if circuit breaker is OPEN
     */
    public boolean isOpen(Object circuitBreaker) {
        if (circuitBreaker == null) {
            return false;
        }
        
        try {
            Method getStateMethod = circuitBreaker.getClass().getMethod("getState");
            Object state = getStateMethod.invoke(circuitBreaker);
            
            Method nameMethod = state.getClass().getMethod("name");
            String stateName = (String) nameMethod.invoke(state);
            
            return "OPEN".equals(stateName);
        } catch (Exception e) {
            logger.debug("failed to check circuit breaker state: {}", e.getMessage());
            return false;
        }
    }

    /**
     * Executes the join point with circuit breaker tracking.
     * 
     * @param circuitBreaker the circuit breaker instance
     * @param pjp the proceeding join point
     * @return the result of execution
     * @throws Throwable if execution fails
     */
    public Object execute(Object circuitBreaker, ProceedingJoinPoint pjp) throws Throwable {
        try {
            Method executeSupplierMethod = circuitBreaker.getClass().getMethod("executeSupplier", 
                Supplier.class);
            
            Supplier<Object> supplier = () -> {
                try {
                    return pjp.proceed();
                } catch (Throwable throwable) {
                    if (throwable instanceof RuntimeException) {
                        throw (RuntimeException) throwable;
                    }
                    throw new RuntimeException(throwable);
                }
            };
            
            return executeSupplierMethod.invoke(circuitBreaker, supplier);
        } catch (Exception e) {
            logger.warn("circuit breaker execution failed, falling back to normal execution: {}", e.getMessage());
            return pjp.proceed();
        }
    }

    /**
     * Checks if circuit breaker functionality is available.
     * 
     * @return true if circuit breaker service is available
     */
    public boolean isAvailable() {
        return circuitBreakerService != null && circuitBreakerService.isAvailable();
    }
}

