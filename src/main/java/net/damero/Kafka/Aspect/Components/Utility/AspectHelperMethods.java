package net.damero.Kafka.Aspect.Components.Utility;

import lombok.experimental.UtilityClass;
import net.damero.Kafka.Annotations.CustomKafkaListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.springframework.context.ApplicationContext;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;


/*
    METHODS USED BY KafkaListenerAspect
 */
@UtilityClass
public class AspectHelperMethods {

    private static final Logger logger = org.slf4j.LoggerFactory.getLogger(AspectHelperMethods.class);


    public KafkaTemplate<?, ?> resolveKafkaTemplate(CustomKafkaListener customKafkaListener, ApplicationContext context, KafkaTemplate<?, ?> defaultKafkaTemplate) {
        Class<?> templateClass = customKafkaListener.kafkaTemplate();

        if (templateClass.equals(void.class)) {
            return defaultKafkaTemplate;
        }

        try {
            return (KafkaTemplate<?, ?>) context.getBean(templateClass);
        } catch (Exception e) {
            logger.warn("failed to resolve custom kafka template {}, using default: {}",
                    templateClass.getName(), e.getMessage());
            return defaultKafkaTemplate;
        }
    }

    public Acknowledgment extractAcknowledgment(Object[] args) {
        for (Object arg : args) {
            if (arg instanceof Acknowledgment) {
                return (Acknowledgment) arg;
            }
        }
        return null;
    }

    /**
     * Extracts ConsumerRecord from method arguments if present.
     *
     * @param arg the first argument from the join point
     * @return ConsumerRecord if present, null otherwise
     */
    @SuppressWarnings("unchecked")
    public ConsumerRecord<?, ?> extractConsumerRecord(Object arg) {
        if (arg instanceof ConsumerRecord<?, ?>) {
            return (ConsumerRecord<?, ?>) arg;
        }
        return null;
    }

    /**
     * Checks if an exception is non-retryable based on the configured nonRetryableExceptions.
     * If nonRetryableExceptions is empty, all exceptions are retryable.
     *
     * @param exception the exception to check
     * @param customKafkaListener the listener configuration
     * @return true if the exception is non-retryable (should go to DLQ), false if it should be retried
     */

    public boolean isNonRetryableException(Exception exception, CustomKafkaListener customKafkaListener) {
        Class<? extends Throwable>[] nonRetryableExceptions = customKafkaListener.nonRetryableExceptions();

        // If no non-retryable exceptions specified, all exceptions are retryable
        if (nonRetryableExceptions == null || nonRetryableExceptions.length == 0) {
            return false;
        }

        Class<?> exceptionClass = exception.getClass();
        for (Class<? extends Throwable> nonRetryableException : nonRetryableExceptions) {
            if (nonRetryableException != null && nonRetryableException.isAssignableFrom(exceptionClass)) {
                return true;
            }
        }

        return false;
    }
}
