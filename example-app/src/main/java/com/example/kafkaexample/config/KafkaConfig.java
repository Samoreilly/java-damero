package com.example.kafkaexample.config;

import com.example.kafkaexample.model.OrderEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.util.backoff.FixedBackOff;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConfig {

    @Bean
    public ObjectMapper kafkaObjectMapper() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        mapper.registerModule(new com.fasterxml.jackson.datatype.jsr310.JavaTimeModule());
        
        // Enable polymorphic type handling for EventWrapper (required for DLQ)
        com.fasterxml.jackson.databind.jsontype.BasicPolymorphicTypeValidator ptv = 
            com.fasterxml.jackson.databind.jsontype.BasicPolymorphicTypeValidator.builder()
                .allowIfBaseType(Object.class)
                .build();
        mapper.activateDefaultTyping(ptv, com.fasterxml.jackson.databind.ObjectMapper.DefaultTyping.NON_FINAL);
        
        return mapper;
    }

    @Bean(name = "kafkaListenerContainerFactory")
    public ConcurrentKafkaListenerContainerFactory<String, OrderEvent> kafkaListenerContainerFactory(
            ObjectMapper kafkaObjectMapper) {
        
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "order-processor");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest"); // Start from latest to avoid old messages

        JsonDeserializer<OrderEvent> jsonDeserializer = new JsonDeserializer<>(OrderEvent.class, kafkaObjectMapper);
        jsonDeserializer.addTrustedPackages("*");
        jsonDeserializer.setUseTypeHeaders(false); // Disable type headers to avoid issues with old messages

        // Wrap with ErrorHandlingDeserializer to handle deserialization errors gracefully
        ErrorHandlingDeserializer<OrderEvent> errorHandlingDeserializer = 
            new ErrorHandlingDeserializer<>(jsonDeserializer);

        ConsumerFactory<String, OrderEvent> consumerFactory = new DefaultKafkaConsumerFactory<>(
                props,
                new StringDeserializer(),
                errorHandlingDeserializer
        );

        ConcurrentKafkaListenerContainerFactory<String, OrderEvent> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        
        // CRITICAL: Use error handler with 0 retries so our aspect can handle exceptions
        // The aspect handles all retry/DLQ logic, so Spring Kafka should not retry
        // Using DefaultErrorHandler with FixedBackOff(0, 0) prevents Spring Kafka from retrying
        factory.setCommonErrorHandler(new DefaultErrorHandler(new FixedBackOff(0, 0)));
        
        // Enable manual ack mode
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);

        return factory;
    }

    @Bean(name = "dlqKafkaListenerContainerFactory")
    public ConcurrentKafkaListenerContainerFactory<String, net.damero.Kafka.CustomObject.EventWrapper<?>> dlqKafkaListenerContainerFactory(
            ObjectMapper kafkaObjectMapper) {
        
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "dlq-monitor");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest"); // Start from latest to avoid old messages
        
        JsonDeserializer<net.damero.Kafka.CustomObject.EventWrapper<?>> jsonDeserializer = 
            new JsonDeserializer<>(net.damero.Kafka.CustomObject.EventWrapper.class, kafkaObjectMapper);
        jsonDeserializer.addTrustedPackages("*");
        jsonDeserializer.setUseTypeHeaders(false); // Disable type headers to avoid issues with old messages

        // Wrap with ErrorHandlingDeserializer to handle deserialization errors gracefully
        ErrorHandlingDeserializer<net.damero.Kafka.CustomObject.EventWrapper<?>> errorHandlingDeserializer = 
            new ErrorHandlingDeserializer<>(jsonDeserializer);

        ConsumerFactory<String, net.damero.Kafka.CustomObject.EventWrapper<?>> consumerFactory = 
            new DefaultKafkaConsumerFactory<>(
                props,
                new StringDeserializer(),
                errorHandlingDeserializer
            );

        ConcurrentKafkaListenerContainerFactory<String, net.damero.Kafka.CustomObject.EventWrapper<?>> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.setCommonErrorHandler(null);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);

        return factory;
    }

    @Bean
    public ProducerFactory<String, OrderEvent> producerFactory(ObjectMapper kafkaObjectMapper) {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

        JsonSerializer<OrderEvent> jsonSerializer = new JsonSerializer<>(kafkaObjectMapper);
        jsonSerializer.setAddTypeInfo(false); // Disable type headers to match consumer config

        return new DefaultKafkaProducerFactory<>(props, new StringSerializer(), jsonSerializer);
    }

    @Bean
    public KafkaTemplate<String, OrderEvent> kafkaTemplate(ProducerFactory<String, OrderEvent> producerFactory) {
        return new KafkaTemplate<>(producerFactory);
    }
}

