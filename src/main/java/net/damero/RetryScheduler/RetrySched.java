package net.damero.RetryScheduler;
import net.damero.Annotations.CustomKafkaListener;
import net.damero.CustomKafkaSetup.DelayMethod;
import net.damero.CustomObject.EventMetadata;
import net.damero.CustomObject.EventWrapper;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.stereotype.Component;

import java.time.Instant;

@Component
public class RetrySched {

    @Autowired
    private final TaskScheduler taskScheduler;

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
        System.out.println("SCHEDULED RETRY:" + eventWrapper.getEvent());
    }


    public void executeRetry(CustomKafkaListener customKafkaListener, EventWrapper<?> eventWrapper, KafkaTemplate<?, ?> kafkaTemplate) {
        // Resend the wrapper so metadata (attempts, timestamps) is preserved across retries
        sendToTopic(kafkaTemplate, customKafkaListener.topic(), eventWrapper);
        System.out.println("SEND TO TOPIC CALLED IN EXECUTE RETRY");
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