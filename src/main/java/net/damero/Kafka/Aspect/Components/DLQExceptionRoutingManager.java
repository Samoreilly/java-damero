package net.damero.Kafka.Aspect.Components;


import net.damero.Kafka.Aspect.Components.Utility.EventUnwrapper;
import net.damero.Kafka.Tracing.TracingSpan;
import net.damero.Kafka.Annotations.CustomKafkaListener;
import net.damero.Kafka.Annotations.DlqExceptionRoutes;
import net.damero.Kafka.CustomObject.EventMetadata;
import net.damero.Kafka.Tracing.TracingService;
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
    private final TracingService tracingService;


    public DLQExceptionRoutingManager(DLQRouter dlqRouter,
                                      RetryOrchestrator retryOrchestrator,
                                      TracingService tracingService){
        this.dlqRouter = dlqRouter;
        this.retryOrchestrator = retryOrchestrator;
        this.tracingService = tracingService;
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

            TracingSpan routingSpan = null;
            if (customKafkaListener.openTelemetry()) {
                String eventId = EventUnwrapper.extractEventId(originalEvent);
                routingSpan = tracingService.startDLQSpan(
                    customKafkaListener.topic(),
                    dlqTopic,
                    eventId,
                    1,
                    "conditional_routing_skip_retry"
                );
                routingSpan.setAttribute("damero.exception.type", e.getClass().getSimpleName());
                routingSpan.setAttribute("damero.dlq.skip_retry", true);
            }

            try {
                dlqRouter.sendToDLQAfterMaxAttempts(
                        kafkaTemplate,
                        originalEvent,
                        e,
                        1,  // First attempt, no retries
                        existingMetadata,
                        dlqTopic,
                        customKafkaListener
                );

                if (customKafkaListener.openTelemetry() && routingSpan != null) {
                    routingSpan.setSuccess();
                }

                return true;  // Message handled, stop processing
            } catch (Exception routingException) {
                if (customKafkaListener.openTelemetry() && routingSpan != null) {
                    routingSpan.recordException(routingException);
                }
                throw routingException;
            } finally {
                if (customKafkaListener.openTelemetry() && routingSpan != null) {
                    routingSpan.end();
                }
            }
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
        boolean isConditionalRoute = false;

        if(matchedRoute != null && !matchedRoute.skipRetry()){
            // Send to conditional DLQ for exceptions that were retried
            targetDlq = matchedRoute.dlqExceptionTopic();
            isConditionalRoute = true;
            logger.info("Routing to conditional DLQ '{}' after {} attempts", targetDlq, currentAttempts);
        } else {
            // Fallback to default DLQ
            targetDlq = customKafkaListener.dlqTopic();
            logger.info("Routing to default DLQ '{}' after {} attempts", targetDlq, currentAttempts);
        }

        TracingSpan routingSpan = null;
        if (customKafkaListener.openTelemetry()) {
            routingSpan = tracingService.startDLQSpan(
                customKafkaListener.topic(),
                targetDlq,
                eventId,
                currentAttempts,
                isConditionalRoute ? "conditional_routing_after_retry" : "default_routing"
            );
            routingSpan.setAttribute("damero.exception.type", e.getClass().getSimpleName());
            routingSpan.setAttribute("damero.dlq.conditional_route", isConditionalRoute);
        }

        try {
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

            if (customKafkaListener.openTelemetry() && routingSpan != null) {
                routingSpan.setSuccess();
            }
        } catch (Exception routingException) {
            if (customKafkaListener.openTelemetry() && routingSpan != null) {
                routingSpan.recordException(routingException);
            }
            throw routingException;
        } finally {
            if (customKafkaListener.openTelemetry() && routingSpan != null) {
                routingSpan.end();
            }
        }
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
