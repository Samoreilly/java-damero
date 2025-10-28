//package net.damero.config;
//
//import com.fasterxml.jackson.databind.ObjectMapper;
//import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
//import net.damero.CustomObject.EventWrapper;
//import org.apache.kafka.clients.consumer.ConsumerConfig;
//import org.apache.kafka.clients.producer.ProducerConfig;
//import org.apache.kafka.common.serialization.StringDeserializer;
//import org.apache.kafka.common.serialization.StringSerializer;
//import org.springframework.beans.factory.annotation.Value;
//import org.springframework.context.annotation.Bean;
//import org.springframework.context.annotation.Configuration;
//import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
//import org.springframework.kafka.core.*;
//import org.springframework.kafka.support.serializer.JsonDeserializer;
//import org.springframework.kafka.support.serializer.JsonSerializer;
//
//import java.util.HashMap;
//import java.util.Map;
//
//@Configuration
//public class KafkaConfig {
//
//    @Value("${spring.kafka.bootstrap-servers:localhost:9092}")
//    private String bootstrapServers;
//
//    @Bean
//    public ObjectMapper kafkaObjectMapper() {
//        ObjectMapper mapper = new ObjectMapper();
//        mapper.registerModule(new JavaTimeModule());
//        return mapper;
//    }
//
//    @Bean
//    public ProducerFactory<String, Object> producerFactory(ObjectMapper kafkaObjectMapper) {
//        Map<String, Object> config = new HashMap<>();
//        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
//        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
//        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
//        config.put(JsonSerializer.ADD_TYPE_INFO_HEADERS, false); // keep only this
//
//        return new DefaultKafkaProducerFactory<>(config);
//    }
//
//
//    @Bean
//    public KafkaTemplate<String, Object> kafkaTemplate(ProducerFactory<String, Object> producerFactory) {
//        return new KafkaTemplate<>(producerFactory);
//    }
//
//    @Bean
//    public ConsumerFactory<String, Object> consumerFactory(ObjectMapper kafkaObjectMapper) {
//        Map<String, Object> config = new HashMap<>();
//        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
//        config.put(ConsumerConfig.GROUP_ID_CONFIG, "default-group");
//        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
//        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
//        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
//        config.put(JsonDeserializer.TRUSTED_PACKAGES, "*");
//        // REMOVE: config.put(JsonDeserializer.USE_TYPE_INFO_HEADERS, false);
//
//        JsonDeserializer<Object> jsonDeserializer = new JsonDeserializer<>(Object.class, kafkaObjectMapper);
//        jsonDeserializer.addTrustedPackages("*");
//        jsonDeserializer.setUseTypeHeaders(false);
//
//        return new DefaultKafkaConsumerFactory<>(
//                config,
//                new StringDeserializer(),
//                jsonDeserializer
//        );
//    }
//
//    @Bean(name = "kafkaListenerContainerFactory")
//    public ConcurrentKafkaListenerContainerFactory<String, Object> kafkaListenerContainerFactory(
//            ConsumerFactory<String, Object> consumerFactory) {
//        ConcurrentKafkaListenerContainerFactory<String, Object> factory =
//                new ConcurrentKafkaListenerContainerFactory<>();
//        factory.setConsumerFactory(consumerFactory);
//        return factory;
//    }
//
//    @Bean(name = "dlqKafkaListenerContainerFactory")
//    public ConsumerFactory<String, EventWrapper<Object>> dlqConsumerFactory(ObjectMapper mapper) {
//        @SuppressWarnings("unchecked")
//        JsonDeserializer<EventWrapper<Object>> deserializer =
//                (JsonDeserializer<EventWrapper<Object>>) (JsonDeserializer<?>)
//                        new JsonDeserializer<>(EventWrapper.class, mapper);
//
//        deserializer.addTrustedPackages("*");
//        deserializer.setUseTypeHeaders(false);
//
//        Map<String, Object> props = new HashMap<>();
//        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
//        props.put(ConsumerConfig.GROUP_ID_CONFIG, "dlq-group");
//        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
//
//        return new DefaultKafkaConsumerFactory<>(
//                props,
//                new StringDeserializer(),
//                deserializer
//        );
//    }
//
//
//    @Bean
//    public ConcurrentKafkaListenerContainerFactory<String, EventWrapper<Object>> dlqKafkaListenerContainerFactory(
//            ConsumerFactory<String, EventWrapper<Object>> dlqConsumerFactory) {
//        ConcurrentKafkaListenerContainerFactory<String, EventWrapper<Object>> factory =
//                new ConcurrentKafkaListenerContainerFactory<>();
//        factory.setConsumerFactory(dlqConsumerFactory);
//        return factory;
//    }
//
//
//    // Alias for backward compatibility
//    @Bean(name = "defaultFactory")
//    public ConcurrentKafkaListenerContainerFactory<String, Object> defaultFactory(
//            ConsumerFactory<String, Object> consumerFactory) {
//        return kafkaListenerContainerFactory(consumerFactory);
//    }
//
//    // Alias for KafkaTemplate
//    @Bean(name = "defaultKafkaTemplate")
//    public KafkaTemplate<String, Object> defaultKafkaTemplate(ProducerFactory<String, Object> producerFactory) {
//        return kafkaTemplate(producerFactory);
//    }
//}