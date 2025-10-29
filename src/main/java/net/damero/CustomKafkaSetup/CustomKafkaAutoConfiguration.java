package net.damero.CustomKafkaSetup;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.BasicPolymorphicTypeValidator;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import net.damero.CustomKafkaSetup.RetryKafkaListener.KafkaRetryListener;
import net.damero.CustomKafkaSetup.RetryKafkaListener.RetryDelayCalculator;
import net.damero.CustomObject.EventMetadata;
import net.damero.CustomObject.EventWrapper;
import net.damero.KafkaServices.KafkaDLQ;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.HashMap;
import java.util.Map;

/**
 * Auto-configuration for CustomKafka library.
 * Provides default beans that users can override if needed.
 *
 * Can be disabled by setting: custom.kafka.auto-config.enabled=false
 */
@AutoConfiguration
@ConditionalOnProperty(
        prefix = "custom.kafka.auto-config",
        name = "enabled",
        havingValue = "true",
        matchIfMissing = true
)
public class CustomKafkaAutoConfiguration {

    @Value("${spring.kafka.bootstrap-servers:localhost:9092}")
    private String bootstrapServers;

    @Bean
    @ConditionalOnMissingBean
    public KafkaDLQ kafkaDLQ() {
        return new KafkaDLQ();
    }

    @Bean
    @ConditionalOnMissingBean
    public KafkaRetryListener kafkaRetryListener(
            KafkaTemplate<String, Object> defaultKafkaTemplate,
            RetryDelayCalculator delayCalculator) {
        return new KafkaRetryListener(defaultKafkaTemplate, delayCalculator);
    }

    @Bean
    @ConditionalOnMissingBean
    public RetryDelayCalculator retryDelayCalculator() {
        return new RetryDelayCalculator();
    }

    @Bean
    @ConditionalOnMissingBean
    public KafkaListenerAspect kafkaListenerAspect(
            KafkaDLQ kafkaDLQ,
            ApplicationContext context,
            KafkaTemplate<?, ?> defaultKafkaTemplate) {
        return new KafkaListenerAspect(kafkaDLQ, context, defaultKafkaTemplate);
    }

    /**
     * Provides a pre-configured ObjectMapper for Kafka serialization.
     * Users can override this bean to customize serialization behavior.
     */
    @Bean
    @ConditionalOnMissingBean(name = "kafkaObjectMapper")
    public ObjectMapper kafkaObjectMapper() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());

        // Enable polymorphic type handling for EventWrapper
        BasicPolymorphicTypeValidator ptv = BasicPolymorphicTypeValidator.builder()
                .allowIfBaseType(Object.class)
                .build();
        mapper.activateDefaultTyping(ptv, ObjectMapper.DefaultTyping.NON_FINAL);

        return mapper;
    }

    @Bean
    @ConditionalOnMissingBean(name = "retryContainerFactory")
    public ConcurrentKafkaListenerContainerFactory<String, EventWrapper<?>> retryContainerFactory(
            ObjectMapper kafkaObjectMapper) {

        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "kafka-retry-group");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        JsonDeserializer<EventWrapper<?>> deserializer =
                new JsonDeserializer<>(EventWrapper.class, kafkaObjectMapper);
        deserializer.addTrustedPackages("*");
        deserializer.setUseTypeHeaders(false);

        ConsumerFactory<String, EventWrapper<?>> consumerFactory = new DefaultKafkaConsumerFactory<>(
                props,
                new StringDeserializer(),
                deserializer
        );

        ConcurrentKafkaListenerContainerFactory<String, EventWrapper<?>> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.setCommonErrorHandler(null);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);

        return factory;
    }

    /**
     * Default KafkaTemplate for sending to DLQ and other internal operations.
     * Users typically don't need to override this.
     */
    @Bean
    @ConditionalOnMissingBean(name = "defaultKafkaTemplate")
    public KafkaTemplate<String, Object> defaultKafkaTemplate(ObjectMapper kafkaObjectMapper) {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

        DefaultKafkaProducerFactory<String, Object> factory = new DefaultKafkaProducerFactory<>(props);
        JsonSerializer<Object> serializer = new JsonSerializer<>(kafkaObjectMapper);
        serializer.setAddTypeInfo(true);
        factory.setValueSerializer(serializer);

        return new KafkaTemplate<>(factory);
    }

    /**
     * Default factory for internal use by the library.
     * Users typically don't need to interact with this.
     */
    @Bean
    @ConditionalOnMissingBean(name = "defaultFactory")
    public ConcurrentKafkaListenerContainerFactory<String, Object> defaultFactory(
            ObjectMapper kafkaObjectMapper) {

        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "custom-kafka-default-group");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        JsonDeserializer<Object> jsonDeserializer = new JsonDeserializer<>(kafkaObjectMapper);
        jsonDeserializer.addTrustedPackages("*");
        jsonDeserializer.setUseTypeHeaders(false);

        ConsumerFactory<String, Object> consumerFactory = new DefaultKafkaConsumerFactory<>(
                props,
                new StringDeserializer(),
                jsonDeserializer
        );

        ConcurrentKafkaListenerContainerFactory<String, Object> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);

        // Disable container-level error handling - let aspect handle retries
        factory.setCommonErrorHandler(null);

        // Enable manual ack mode for proper message acknowledgment
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);

        return factory;
    }

    /**
     * Consumer factory for DLQ messages (EventWrapper).
     * Automatically handles deserialization of wrapped events.
     */
    @Bean
    @ConditionalOnMissingBean(name = "dlqConsumerFactory")
    public ConsumerFactory<String, EventWrapper<?>> dlqConsumerFactory(ObjectMapper kafkaObjectMapper) {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "custom-kafka-dlq-group");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        JsonDeserializer<EventWrapper<?>> deserializer =
                new JsonDeserializer<>(EventWrapper.class, kafkaObjectMapper);
        deserializer.addTrustedPackages("*");
        deserializer.setUseTypeHeaders(false);

        return new DefaultKafkaConsumerFactory<>(
                props,
                new StringDeserializer(),
                deserializer
        );
    }

    /**
     * Container factory for DLQ listeners.
     * Use this factory in your @KafkaListener for DLQ topics:
     * @KafkaListener(topics = "my-dlq", containerFactory = "dlqKafkaListenerContainerFactory")
     */
    @Bean
    @ConditionalOnMissingBean(name = "dlqKafkaListenerContainerFactory")
    public ConcurrentKafkaListenerContainerFactory<String, EventWrapper<?>> dlqKafkaListenerContainerFactory(
            ConsumerFactory<String, EventWrapper<?>> dlqConsumerFactory) {
        ConcurrentKafkaListenerContainerFactory<String, EventWrapper<?>> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(dlqConsumerFactory);
        return factory;
    }
}