package net.damero.Kafka.RetryScheduler;

import net.damero.Kafka.Annotations.CustomKafkaListener;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@Component
public class RetrySched {

    @Autowired
    private final TaskScheduler taskScheduler;
    private static final Logger logger = LoggerFactory.getLogger(RetrySched.class);

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

        long delay = getBackOffDelay(customKafkaListener, customKafkaListener.delayMethod(), currentAttempts);

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
        // Send the original event with headers attached
        sendToTopicWithHeaders(kafkaTemplate, customKafkaListener.topic(), originalEvent, headers);
        logger.info("retried event: {} to topic: {}", originalEvent.toString(), customKafkaListener.topic());
    }

    private long getBackOffDelay(CustomKafkaListener customKafkaListener, DelayMethod delayMethod, int attempts) {

        double base = customKafkaListener.delay();

        return (long) switch (delayMethod) {
            case EXPO -> {

                long expoDelay = (long) (base * Math.pow(2, attempts));

                //max 5 seconds delay
                yield Math.min(expoDelay, 5000);
            }

            case LINEAR -> base * attempts;
            case CUSTOM -> customKafkaListener.delay();
            case MAX -> base;
            default -> base;
        };
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