package java.damero.CustomKafkaSetup;

import lombok.Getter;

import java.damero.annotations.CustomKafkaListener;

@Getter
public class CustomKafkaListenerConfig {

    private final String topic;
    private final String dlqTopic;
    private final int maxAttempts;
    private final double delay;


    CustomKafkaListenerConfig(Builder builder) {//takes in builder config
        this.topic = builder.topic;
        this.dlqTopic = builder.dlqTopic;
        this.maxAttempts = builder.maxAttempts;
        this.delay = builder.delay;
    }


    public static CustomKafkaListenerConfig fromAnnotation(CustomKafkaListener annotation) {
        return new Builder()
                .topic(annotation.topic())
                .dlqTopic(annotation.dlqTopic())
                .maxAttempts(annotation.maxAttempts())
                .delay(annotation.delay())
                .build();
    }
    //Builder class used to build custom Kafka Listener config class, will add more ltr

    public static class Builder {

        private String topic;
        private String dlqTopic;
        private int maxAttempts;
        private double delay;

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

        //custom kafkalistenerconfig takes in builder config

        public CustomKafkaListenerConfig build() {
            return new CustomKafkaListenerConfig(this);
        }
    }

}
