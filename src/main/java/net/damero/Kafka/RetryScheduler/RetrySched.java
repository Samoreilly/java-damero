package net.damero.Kafka.RetryScheduler;

import net.damero.Kafka.Annotations.DameroKafkaListener;
import net.damero.Kafka.Config.DelayMethod;
import net.damero.Kafka.Aspect.Components.Utility.HeaderUtils;
import net.damero.Kafka.Config.PluggableRedisCache;
import net.damero.Kafka.CustomObject.EventMetadata;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.stereotype.Component;

import java.time.Instant;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static net.damero.Kafka.Aspect.Components.Utility.EventUnwrapper.extractEventId;


@Component
public class RetrySched {

    private static final Logger logger = LoggerFactory.getLogger(RetrySched.class);

    private final TaskScheduler taskScheduler;
    private final PluggableRedisCache cache;


    public RetrySched(TaskScheduler taskScheduler, PluggableRedisCache cache) {
        this.taskScheduler = taskScheduler;
        this.cache = cache;
    }

    /**
     * Schedules a retry for the given event with headers-based metadata.
     * 
     * @param dameroKafkaListener the listener configuration
     * @param originalEvent the original event to retry
     * @param headers headers containing retry metadata
     * @param kafkaTemplate the Kafka template to use
     */
    public void scheduleRetry(DameroKafkaListener dameroKafkaListener,
                              Object originalEvent,
                              Headers headers,
                              KafkaTemplate<?, ?> kafkaTemplate) {

        // Extract current attempts from headers
        EventMetadata metadata = HeaderUtils.extractMetadataFromHeaders(headers);
        int currentAttempts = metadata != null ? metadata.getAttempts() : 0;

        long delay = getBackOffDelay(dameroKafkaListener, originalEvent,
                                     dameroKafkaListener.delayMethod(), currentAttempts);

        taskScheduler.schedule(() -> executeRetry(dameroKafkaListener, originalEvent, headers, kafkaTemplate),
                Instant.now().plusMillis(delay)
        );

        logger.info("scheduled retry for event: {} to topic: {}", originalEvent.toString(), dameroKafkaListener.topic());
    }

    /**
     * Executes the retry by sending the event with headers to the topic.
     */
    public void executeRetry(DameroKafkaListener dameroKafkaListener,
                             Object originalEvent,
                             Headers headers,
                             KafkaTemplate<?, ?> kafkaTemplate) {
        try {
            // Send the original event with headers attached
            sendToTopicWithHeaders(kafkaTemplate, dameroKafkaListener.topic(), originalEvent, headers);
            logger.info("retried event: {} to topic: {}", originalEvent.toString(), dameroKafkaListener.topic());
        } catch (Exception e) {
            // Log error but don't throw - allows scheduled task to complete even if Kafka is unavailable
            // This prevents test hangs when embedded Kafka shuts down before scheduled retries execute
            logger.error("failed to execute retry for event: {} to topic: {} - {}", 
                originalEvent != null ? originalEvent.toString() : "null", 
                dameroKafkaListener.topic(),
                e.getMessage(), 
                e);
        }

    }

    private long getBackOffDelay(DameroKafkaListener dameroKafkaListener, Object object, DelayMethod delayMethod, int attempts) {
        if(delayMethod == DelayMethod.FIBONACCI && object == null){
            logger.warn("Object cannot be null for FIBONACCI delay method");
            throw new IllegalArgumentException("Object cannot be null for FIBONACCI delay method");
        }

        double base = dameroKafkaListener.delay();

        return (long) switch (delayMethod) {
            case EXPO -> {

                long expoDelay = (long) (base * Math.pow(2, attempts));

                //max 5 seconds delay
                yield Math.min(expoDelay, 5000);
            }

            case LINEAR -> base * attempts;
            case FIBONACCI -> getFibonacciDelay(dameroKafkaListener, object);
            case CUSTOM -> dameroKafkaListener.delay();
            case MAX -> base;
            default -> base;
        };
    }

    private long getFibonacciDelay(DameroKafkaListener dameroKafkaListener, Object object) {
        String eventId = extractEventId(object);
        if (eventId == null) {
            logger.warn("Cannot get fibonacci delay - eventId is null, using default");
            return (long) dameroKafkaListener.delay();
        }
        return cache.getNextFibonacciDelay(eventId, dameroKafkaListener.fibonacciLimit());
    }

    /**
     * Clean up Fibonacci cache for a specific event.
     * Call this when event processing completes successfully.
     */
    public void clearFibonacciState(Object event) {
        if (event != null) {
            String eventId = extractEventId(event);
            cache.clearFibonacciState(eventId);
        }
    }

    /**
     * Sends a message to a Kafka topic with headers.
     */
    @SuppressWarnings("unchecked")
    private <K, V> void sendToTopicWithHeaders(KafkaTemplate<?, ?> template, String topic, V message, Headers headers) {
        try {
            ProducerRecord<K, V> record =
                new ProducerRecord<>(topic, null, null, null, message, headers);
            ((KafkaTemplate<K, V>) template).send(record);
        } catch (Exception e) {
            throw new RuntimeException("failed to send to kafka topic: " + topic, e);
        }
    }

    @SuppressWarnings("unchecked")
    private <K, V> void sendToTopic(KafkaTemplate<?, ?> template, String topic, V message) {
        try {
            ((KafkaTemplate<K, V>) template).send(topic, message);
        } catch (Exception e) {
            throw new RuntimeException("Failed to send to Kafka topic: " + topic, e);
        }
    }
}