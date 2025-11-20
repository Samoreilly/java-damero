package net.damero.Kafka.Aspect.Components;


import net.damero.Kafka.Annotations.CustomKafkaListener;
import net.damero.Kafka.Annotations.DlqExceptionRoutes;
import net.damero.Kafka.CustomObject.EventMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;


/**
 * Manages conditional DLQ routing based on exception types.
 * Routes exceptions to specific DLQ topics based on annotation configuration.
 */
public class DLQExceptionRoutingManager {

    private static final Logger logger = LoggerFactory.getLogger(DLQExceptionRoutingManager.class);

    private final DLQRouter dlqRouter;
    private final RetryOrchestrator retryOrchestrator;


    public DLQExceptionRoutingManager(DLQRouter dlqRouter, RetryOrchestrator retryOrchestrator){
        this.dlqRouter = dlqRouter;
        this.retryOrchestrator = retryOrchestrator;
    }

    /**
     * Routes exception to DLQ immediately if skipRetry=true, otherwise returns null.
     * Used during initial exception handling to check if retry should be skipped.
     *
     * @return true if message was routed to DLQ (skipRetry=true), false if should retry
     */
    public boolean routeToDLQIfSkipRetry(CustomKafkaListener customKafkaListener,
                                         KafkaTemplate<?, ?> kafkaTemplate,
                                         Object originalEvent,
                                         Exception e,
                                         EventMetadata existingMetadata){

        DlqExceptionRoutes matchedRoute = findMatchingRoute(customKafkaListener, e);

        if(matchedRoute != null && matchedRoute.skipRetry()){
            String dlqTopic = matchedRoute.dlqExceptionTopic();

            logger.info("Exception {} matched DLQ route with skipRetry=true - sending to {} without retry",
                e.getClass().getSimpleName(), dlqTopic);

            dlqRouter.sendToDLQAfterMaxAttempts(
                    kafkaTemplate,
                    originalEvent,
                    e,
                    1,  // First attempt, no retries
                    existingMetadata,
                    dlqTopic,
                    customKafkaListener
            );

            return true;  // Message handled, stop processing
        }

        return false;  // Should continue with retry logic
    }

    /**
     * Routes exception to conditional DLQ after max retry attempts.
     * If no matching route found, falls back to default DLQ.
     */
    public void routeToDLQAfterMaxAttempts(CustomKafkaListener customKafkaListener,
                                           KafkaTemplate<?, ?> kafkaTemplate,
                                           Object originalEvent,
                                           Exception e,
                                           String eventId,
                                           int currentAttempts,
                                           EventMetadata existingMetadata){

        DlqExceptionRoutes matchedRoute = findMatchingRoute(customKafkaListener, e);
        String targetDlq;

        if(matchedRoute != null && !matchedRoute.skipRetry()){
            // Send to conditional DLQ for exceptions that were retried
            targetDlq = matchedRoute.dlqExceptionTopic();
            logger.info("Routing to conditional DLQ '{}' after {} attempts", targetDlq, currentAttempts);
        } else {
            // Fallback to default DLQ
            targetDlq = customKafkaListener.dlqTopic();
            logger.info("Routing to default DLQ '{}' after {} attempts", targetDlq, currentAttempts);
        }

        dlqRouter.sendToDLQAfterMaxAttempts(
                kafkaTemplate,
                originalEvent,
                e,
                currentAttempts,
                existingMetadata,
                targetDlq,
                customKafkaListener
        );

        retryOrchestrator.clearAttempts(eventId);
    }

    /**
     * Finds the first matching DLQ route for the given exception.
     * Supports exception inheritance (subclasses match parent types).
     *
     * @return matched route or null if no match found
     */
    private DlqExceptionRoutes findMatchingRoute(CustomKafkaListener customKafkaListener, Exception e) {
        DlqExceptionRoutes[] routes = customKafkaListener.dlqRoutes();

        if(routes == null || routes.length == 0) {
            return null;
        }

        Class<?> exceptionClass = e.getClass();

        for(DlqExceptionRoutes route : routes){
            String dlqTopic = route.dlqExceptionTopic();
            Class<? extends Throwable> routeException = route.exception();  // Single class, not array

            // Validate route configuration
            if(dlqTopic == null || dlqTopic.isEmpty()) {
                logger.warn("DLQ route has empty topic, skipping");
                continue;
            }

            if(routeException == null) {
                logger.warn("DLQ route has null exception class, skipping");
                continue;
            }

            // Check if exception matches
            if(routeException.isAssignableFrom(exceptionClass)){
                logger.debug("Exception {} matched route for DLQ '{}'",
                    exceptionClass.getSimpleName(), dlqTopic);
                return route;  // First match wins
            }
        }

        return null;  // No matching route
    }
}
