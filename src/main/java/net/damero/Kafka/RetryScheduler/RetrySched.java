package net.damero.Kafka.RetryScheduler;

import net.damero.Kafka.Annotations.CustomKafkaListener;
import net.damero.Kafka.CustomKafkaSetup.DelayMethod;
import net.damero.Kafka.CustomObject.EventMetadata;
import net.damero.Kafka.CustomObject.EventWrapper;

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

    //update metadata and calculate backoff delay
    public void scheduleRetry(CustomKafkaListener customKafkaListener, EventWrapper<?> eventWrapper, KafkaTemplate<?, ?> kafkaTemplate) {

        EventMetadata metadata = eventWrapper.getMetadata();
        int currentAttempts = metadata.getAttempts();

        long delay = getBackOffDelay(customKafkaListener, customKafkaListener.delayMethod(), currentAttempts);

        taskScheduler.schedule(() -> executeRetry(customKafkaListener, eventWrapper, kafkaTemplate),
                Instant.now().plusMillis(delay)
        );
        logger.info("Scheduled retry for event: {} to topic: {}", eventWrapper.getEvent().toString(), customKafkaListener.topic());
    }


    public void executeRetry(CustomKafkaListener customKafkaListener, EventWrapper<?> eventWrapper, KafkaTemplate<?, ?> kafkaTemplate) {
        // Resend the wrapper so metadata (attempts, timestamps) is preserved across retries
        sendToTopic(kafkaTemplate, customKafkaListener.topic(), eventWrapper);
        logger.info("Retried event: {} to topic: {}", eventWrapper.getEvent().toString(), customKafkaListener.topic());
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
            case MAX -> Math.min(base, customKafkaListener.delay());
            default -> base;
        };
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