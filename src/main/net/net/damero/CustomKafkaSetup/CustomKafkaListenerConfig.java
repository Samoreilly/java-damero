package net.damero.CustomKafkaSetup;

import lombok.Getter;

import net.damero.annotations.CustomKafkaListener;
import org.springframework.beans.factory.annotation.Value;

@Getter
public class CustomKafkaListenerConfig {

    private final String topic;
    private final String dlqTopic;
    private final int maxAttempts;
    private final double delay;
    private DelayMethod delayMethod;


    CustomKafkaListenerConfig(Builder builder) {//takes in builder config
        this.topic = builder.topic;
        this.dlqTopic = builder.dlqTopic;
        this.maxAttempts = builder.maxAttempts;
        this.delay = builder.delay;
        this.delayMethod = builder.delayMethod;
    }


    public static CustomKafkaListenerConfig fromAnnotation(CustomKafkaListener annotation) {
        return new Builder()
                .topic(annotation.topic())
                .dlqTopic(annotation.dlqTopic())
                .maxAttempts(annotation.maxAttempts())
                .delay(annotation.delay())
                .delayMethod(annotation.delayMethod())
                .build();
    }
    //Builder class used to build custom Kafka Listener config class, will add more ltr

    public static class Builder {

        private String topic;

        private String dlqTopic;

        private int maxAttempts;

        private double delay;

        private DelayMethod delayMethod;//default delay method to linear

        public Builder topic(String topic) {
            this.topic = topic;
            return this;
        }

        public Builder dlqTopic(String dlqTopic) {
            this.dlqTopic = dlqTopic;
            return this;
        }

        public Builder maxAttempts(int maxAttempts) {
            this.maxAttempts = maxAttempts;
            return this;
        }

        public Builder delay(double delay) {
            this.delay = delay;
            return this;
        }

        public Builder delayMethod(DelayMethod delayMethod) {
            this.delayMethod = delayMethod;
            return this;
        }

        //custom kafkalistenerconfig takes in builder config

        public CustomKafkaListenerConfig build() {
            return new CustomKafkaListenerConfig(this);
        }
    }

}
