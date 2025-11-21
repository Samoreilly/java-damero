package net.damero.Kafka.RetryScheduler;

import net.damero.Kafka.Annotations.CustomKafkaListener;
import net.damero.Kafka.Aspect.Components.CaffeineCache;
import net.damero.Kafka.Config.DelayMethod;
import net.damero.Kafka.Aspect.Components.HeaderUtils;
import net.damero.Kafka.CustomObject.EventMetadata;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.stereotype.Component;
import java.time.Instant;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@Component
public class RetrySched {

    private static class FibonacciState {
        private final long[] sequence;
        private int nextIndex;
        private final int maxIndex;

        public FibonacciState(int limit) {
            if (limit < 2) {
                throw new IllegalArgumentException("Fibonacci limit must be at least 2");
            }
            this.sequence = new long[limit];
            this.sequence[0] = 0;
            this.sequence[1] = 1;
            this.nextIndex = 2;
            this.maxIndex = limit - 1;
        }

        public synchronized long getNext() {
            // If we haven't filled the array yet, calculate next value
            if (nextIndex <= maxIndex) {
                //dp[i] = dp[i-1] + dp[i-2]
                sequence[nextIndex] = sequence[nextIndex - 1] + sequence[nextIndex - 2];
                return sequence[nextIndex++];
            }
            // If at limit, return the last (max) value
            return sequence[maxIndex];
        }

        public synchronized long getCurrent() {
            int currentIndex = Math.max(0, nextIndex - 1);
            return sequence[currentIndex];
        }
    }

    @Autowired
    private final TaskScheduler taskScheduler;
    private static final Logger logger = LoggerFactory.getLogger(RetrySched.class);

    private final ConcurrentHashMap<Object, FibonacciState> fibonacciCache = new ConcurrentHashMap<>();

    public RetrySched(TaskScheduler taskScheduler) {
        this.taskScheduler = taskScheduler;
    }

    /**
     * Schedules a retry for the given event with headers-based metadata.
     * 
     * @param customKafkaListener the listener configuration
     * @param originalEvent the original event to retry
     * @param headers headers containing retry metadata
     * @param kafkaTemplate the Kafka template to use
     */
    public void scheduleRetry(CustomKafkaListener customKafkaListener, 
                              Object originalEvent, 
                              Headers headers, 
                              KafkaTemplate<?, ?> kafkaTemplate) {

        // Extract current attempts from headers
        EventMetadata metadata = HeaderUtils.extractMetadataFromHeaders(headers);
        int currentAttempts = metadata != null ? metadata.getAttempts() : 0;

        long delay = getBackOffDelay(customKafkaListener, originalEvent,
                                     customKafkaListener.delayMethod(), currentAttempts);

        taskScheduler.schedule(() -> executeRetry(customKafkaListener, originalEvent, headers, kafkaTemplate),
                Instant.now().plusMillis(delay)
        );

        logger.info("scheduled retry for event: {} to topic: {}", originalEvent.toString(), customKafkaListener.topic());
    }

    /**
     * Executes the retry by sending the event with headers to the topic.
     */
    public void executeRetry(CustomKafkaListener customKafkaListener, 
                            Object originalEvent, 
                            Headers headers, 
                            KafkaTemplate<?, ?> kafkaTemplate) {
        try {
            // Send the original event with headers attached
            sendToTopicWithHeaders(kafkaTemplate, customKafkaListener.topic(), originalEvent, headers);
            logger.info("retried event: {} to topic: {}", originalEvent.toString(), customKafkaListener.topic());
        } catch (Exception e) {
            // Log error but don't throw - allows scheduled task to complete even if Kafka is unavailable
            // This prevents test hangs when embedded Kafka shuts down before scheduled retries execute
            logger.error("failed to execute retry for event: {} to topic: {} - {}", 
                originalEvent != null ? originalEvent.toString() : "null", 
                customKafkaListener.topic(), 
                e.getMessage(), 
                e);
        }
    }

    private long getBackOffDelay(CustomKafkaListener customKafkaListener, Object object, DelayMethod delayMethod, int attempts) {
        if(delayMethod == DelayMethod.FIBONACCI && object == null){
            logger.warn("Object cannot be null for FIBONACCI delay method");
            throw new IllegalArgumentException("Object cannot be null for FIBONACCI delay method");
        }

        double base = customKafkaListener.delay();

        return (long) switch (delayMethod) {
            case EXPO -> {

                long expoDelay = (long) (base * Math.pow(2, attempts));

                //max 5 seconds delay
                yield Math.min(expoDelay, 5000);
            }

            case LINEAR -> base * attempts;
            case FIBONACCI -> getFibonacciDelay(customKafkaListener, object);
            case CUSTOM -> customKafkaListener.delay();
            case MAX -> base;
            default -> base;
        };
    }

    private long getFibonacciDelay(CustomKafkaListener customKafkaListener, Object object){
        // Get or create FibonacciState for this event
        FibonacciState state = fibonacciCache.computeIfAbsent(object,
            k -> new FibonacciState(customKafkaListener.fibonacciLimit()));

        // Get next delay value
        return state.getNext();
    }

    /**
     * Clean up Fibonacci cache for a specific event.
     * Call this when event processing completes successfully.
     */
    public void clearFibonacciState(Object event) {
        if (event != null) {
            fibonacciCache.remove(event);
        }
    }

    /**
     * Sends a message to a Kafka topic with headers.
     */
    @SuppressWarnings("unchecked")
    private <K, V> void sendToTopicWithHeaders(KafkaTemplate<?, ?> template, String topic, V message, Headers headers) {
        try {
            org.apache.kafka.clients.producer.ProducerRecord<K, V> record = 
                new org.apache.kafka.clients.producer.ProducerRecord<>(topic, null, null, null, message, headers);
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