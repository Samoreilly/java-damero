package net.damero.Kafka.Config;

import lombok.Getter;
import net.damero.Kafka.Annotations.CustomKafkaListener;
import net.damero.Kafka.Factory.KafkaConsumerFactoryProvider;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import java.util.List;

@Getter
public class CustomKafkaListenerConfig{


    private final String topic;
    private final String dlqTopic;
    private final int maxAttempts;
    private final double delay;
    private DelayMethod delayMethod;
    private final boolean retryable;
    private final String retryableTopic;
    private final KafkaTemplate<?, ?> kafkaTemplate;
    private final ConsumerFactory<?, ?> consumerFactory;//allow the user to provide custom consumer config
    private final Class<? extends Throwable>[] nonRetryableExceptions;
    
    CustomKafkaListenerConfig(Builder builder) {//takes in builder config
        this.topic = builder.topic;
        this.dlqTopic = builder.dlqTopic;
        this.maxAttempts = builder.maxAttempts;
        this.delay = builder.delay;
        this.delayMethod = builder.delayMethod;
        this.kafkaTemplate = builder.kafkaTemplate;
        this.consumerFactory = builder.consumerFactory;
        this.retryable = builder.retryable;
        this.retryableTopic = builder.retryableTopic;
        this.nonRetryableExceptions = builder.nonRetryableExceptions;
    }

    public static CustomKafkaListenerConfig fromAnnotation(CustomKafkaListener annotation) {


        CustomKafkaListenerConfig.Builder build = new CustomKafkaListenerConfig.Builder();
        build
            .topic(annotation.topic())
            .dlqTopic(annotation.dlqTopic())
            .maxAttempts(annotation.maxAttempts())
            .delay(annotation.delay())
            .delayMethod(annotation.delayMethod())
            .retryable(annotation.retryable())
            .retryableTopic(annotation.retryableTopic())
            .nonRetryableExceptions(annotation.nonRetryableExceptions())
            .consumerFactory(null);

        KafkaTemplate<?, ?> kafkaTemplate = null;

        // validate retryable topic configuration
        if(build.retryable && (build.retryableTopic == null || build.retryableTopic.isEmpty())) {
            throw new IllegalArgumentException("Retryable topic cannot be empty when retryable is true");
        }
        if(build.retryable && build.retryableTopic.equals(build.topic)) {
            throw new IllegalArgumentException("Retryable topic cannot be the same as the topic");
        }
        if(build.retryable && build.retryableTopic.equals(build.dlqTopic)) {
            throw new IllegalArgumentException("Retryable topic cannot be the same as the DLQ topic");
        }
        
        // validate topic and dlqTopic are different
        if(build.topic != null && build.dlqTopic != null && build.topic.equals(build.dlqTopic)) {
            throw new IllegalArgumentException("Topic and DLQ topic cannot be the same");
        }

        build.kafkaTemplate(kafkaTemplate);
        return build.build();
    }
    //Builder class used to build custom Kafka Listener config class, will add more ltr
    
    // Internal factory for aspect to create configs from annotations
    static Builder builder() {
        return new Builder();
    }

    static class Builder<T> {

        private String topic;

        private String dlqTopic;

        private int maxAttempts;

        private double delay;

        private DelayMethod delayMethod;//default delay method to linear

        private boolean retryable;

        private String retryableTopic;

        private Class<? extends Throwable>[] nonRetryableExceptions;


        private KafkaTemplate<?, ?> kafkaTemplate;
        private ConsumerFactory<?, ?> consumerFactory;
        private Class<T> eventType;
        

        Builder topic(String topic) {
            if (topic == null || topic.isEmpty()) {
                throw new IllegalArgumentException("Topic cannot be null or empty");
            }
            this.topic = topic;
            return this;
        }

        Builder dlqTopic(String dlqTopic) {
            if (dlqTopic == null || dlqTopic.isEmpty()) {
                throw new IllegalArgumentException("DLQ topic cannot be null or empty");
            }
            this.dlqTopic = dlqTopic;
            return this;
        }
        Builder eventType(Class<T> eventType){
            this.eventType = eventType;
            return this;
        }

        Builder maxAttempts(int maxAttempts) {
            this.maxAttempts = maxAttempts;
            if (maxAttempts <= 0) {
                throw new IllegalArgumentException("Max attempts must be greater than 0");
            }
            return this;
        }

        Builder delay(double delay) {
            this.delay = delay;
            if (delay < 0) {
                throw new IllegalArgumentException("Delay must be greater than or equal to 0");
            }
            return this;
        }

        Builder delayMethod(DelayMethod delayMethod) {
            this.delayMethod = delayMethod != null ? delayMethod : DelayMethod.EXPO;
            return this;
        }

        Builder retryable(boolean retryable){
            this.retryable = retryable;
            return this;
        }

        Builder retryableTopic(String retryableTopic){
            this.retryableTopic = retryableTopic;
            return this;
        }
        Builder nonRetryableExceptions(Class<? extends Throwable>[] nonRetryableExceptions){
            this.nonRetryableExceptions = nonRetryableExceptions;
            return this;
        }

        Builder kafkaTemplate(KafkaTemplate<?, ?> kafkaTemplate){
            this.kafkaTemplate = kafkaTemplate;
            return this;
        }

        Builder consumerFactory(ConsumerFactory<?, ?> factory) {
            this.consumerFactory = factory;
            return this;
        }

        //custom kafkalistenerconfig takes in builder config

        CustomKafkaListenerConfig build() {
            // validate required fields
            if (topic == null || topic.isEmpty()) {
                throw new IllegalStateException("Topic is required");
            }
            if (dlqTopic == null || dlqTopic.isEmpty()) {
                throw new IllegalStateException("DLQ topic is required");
            }
            if (topic.equals(dlqTopic)) {
                throw new IllegalStateException("Topic and DLQ topic cannot be the same");
            }
            if (maxAttempts <= 0) {
                throw new IllegalStateException("Max attempts must be greater than 0");
            }
            if (delay < 0) {
                throw new IllegalStateException("Delay must be greater than or equal to 0");
            }
            if (delayMethod == null) {
                delayMethod = DelayMethod.EXPO;
            }
            if (retryable && (retryableTopic == null || retryableTopic.isEmpty())) {
                throw new IllegalStateException("Retryable topic is required when retryable is true");
            }
            if (retryable && retryableTopic.equals(topic)) {
                throw new IllegalStateException("Retryable topic cannot be the same as the topic");
            }
            if (retryable && retryableTopic.equals(dlqTopic)) {
                throw new IllegalStateException("Retryable topic cannot be the same as the DLQ topic");
            }


            
            if (consumerFactory == null && eventType != null) {
                // fallback to default consumer factory with generics
                consumerFactory = KafkaConsumerFactoryProvider.defaultConsumerFactory(eventType);
            }
            return new CustomKafkaListenerConfig(this);
        }

    }

}
