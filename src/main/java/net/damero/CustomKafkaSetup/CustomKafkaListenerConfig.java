package net.damero.CustomKafkaSetup;

import lombok.Getter;

import net.damero.Annotations.CustomKafkaListener;
import org.springframework.context.ApplicationContext;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;

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
            .consumerFactory(null);

        KafkaTemplate<?, ?> kafkaTemplate = null;

//        Class<?> factoryClass = annotation.kafkaTemplate();
//        if (!factoryClass.equals(void.class)) {
//            kafkaTemplate = (KafkaTemplate<?, ?>) context.getBean(factoryClass);
//        }

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

        private KafkaTemplate<?, ?> kafkaTemplate;
        private ConsumerFactory<?, ?> consumerFactory;
        private Class<T> eventType;

        Builder topic(String topic) {
            this.topic = topic;
            return this;
        }

        Builder dlqTopic(String dlqTopic) {
            this.dlqTopic = dlqTopic;
            return this;
        }
        Builder eventType(Class<T> evemtType){
            this.eventType = eventType;
            return this;
        }

        Builder maxAttempts(int maxAttempts) {
            this.maxAttempts = maxAttempts;
            return this;
        }

        Builder delay(double delay) {
            this.delay = delay;
            return this;
        }

        Builder delayMethod(DelayMethod delayMethod) {
            this.delayMethod = delayMethod;
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
            if (consumerFactory == null && eventType != null) {
                // fallback to default consumer factory with generics
                consumerFactory = KafkaConsumerFactoryProvider.defaultConsumerFactory(eventType);
            }
            return new CustomKafkaListenerConfig(this);
        }

    }

}
