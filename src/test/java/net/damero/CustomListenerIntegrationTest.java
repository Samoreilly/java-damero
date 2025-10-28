package net.damero;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import net.damero.CustomKafkaSetup.KafkaListenerAspect;
import net.damero.CustomObject.EventWrapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.EnableAspectJAutoProxy;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(SpringExtension.class)
@SpringBootTest(classes = {
        CustomKafkaListenerIntegrationTest.TestConfig.class,
        TestKafkaListener.class,
        DLQMessageCollector.class,
        KafkaListenerAspect.class // Your aspect
})
@EmbeddedKafka(partitions = 1, topics = {"test-topic", "test-dlq"})
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class CustomKafkaListenerIntegrationTest {

    @Autowired
    private EmbeddedKafkaBroker embeddedKafka;

    @Autowired
    private KafkaTemplate<String, TestEvent> kafkaTemplate;

    @Autowired
    private TestKafkaListener testKafkaListener;

    @Autowired
    private DLQMessageCollector dlqCollector;


    @BeforeEach
    void setUp() {
        testKafkaListener.reset();
        dlqCollector.reset();
    }

    @Test
    void testSuccessfulEventProcessing() {
        TestEvent event = new TestEvent(UUID.randomUUID().toString(), "Test message", false);

        // Send with proper key
        kafkaTemplate.send("test-topic", event.getId(), event);  // ADD KEY

        System.out.println("Sent event: " + event);  // ADD LOGGING

        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            System.out.println("Successful events count: " + testKafkaListener.getSuccessfulEvents().size());
            assertEquals(1, testKafkaListener.getSuccessfulEvents().size());
            assertEquals(event.getId(), testKafkaListener.getSuccessfulEvents().get(0).getId());
            assertEquals(1, testKafkaListener.getAttemptCount(event.getId()));
        });
    }

    @Test
    void testFailedEventWithRetries() {
        TestEvent event = new TestEvent(UUID.randomUUID().toString(), "Will fail", true);
        kafkaTemplate.send("test-topic", event);

        await().atMost(15, TimeUnit.SECONDS).untilAsserted(() -> {
            assertEquals(3, testKafkaListener.getAttemptCount(event.getId()), "Should have attempted 3 times");
            assertTrue(testKafkaListener.getFailedEvents().size() >= 3);
        });
    }

    @Test
    void testDLQReceivesMessageAfterMaxAttempts() {
        TestEvent event = new TestEvent(UUID.randomUUID().toString(), "Fail to DLQ", true);
        kafkaTemplate.send("test-topic", event);

        await().atMost(15, TimeUnit.SECONDS).untilAsserted(() -> {
            assertEquals(3, testKafkaListener.getAttemptCount(event.getId()), "Retry logic should have run");
            assertEquals(1, dlqCollector.getReceivedMessages().size(), "DLQ should receive the failed message");
        });
    }

    @Test
    void testDLQMetadataTracksAllAttempts() {
        TestEvent event = new TestEvent(UUID.randomUUID().toString(), "Metadata check", true);
        LocalDateTime testStartTime = LocalDateTime.now();
        kafkaTemplate.send("test-topic", event);

        await().atMost(15, TimeUnit.SECONDS).untilAsserted(() -> {
            assertEquals(1, dlqCollector.getReceivedMessages().size());

            var wrapper = dlqCollector.getReceivedMessages().get(0);
            var metadata = wrapper.getMetadata();

            assertEquals(3, metadata.getAttempts());
            assertNotNull(metadata.getFirstFailureDateTime());
            assertNotNull(metadata.getLastFailureDateTime());
            assertTrue(!metadata.getFirstFailureDateTime().isBefore(testStartTime));
            assertTrue(!metadata.getLastFailureDateTime().isBefore(metadata.getFirstFailureDateTime()));

            TestEvent originalEvent = (TestEvent) wrapper.getEvent();
            assertEquals(event.getId(), originalEvent.getId());
            assertTrue(originalEvent.isShouldFail());
        });
    }

    @Test
    void testSuccessEventDoesNotGoToDLQ() {
        TestEvent event = new TestEvent(UUID.randomUUID().toString(), "Success", false);
        kafkaTemplate.send("test-topic", event);

        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            assertEquals(1, testKafkaListener.getSuccessfulEvents().size());
            assertEquals(0, dlqCollector.getReceivedMessages().size(), "DLQ should be empty for successful events");
        });
    }

    @Configuration
    @EnableKafka
    @EnableAspectJAutoProxy
    static class TestConfig {

        @Bean
        public ObjectMapper kafkaObjectMapper() {
            ObjectMapper mapper = new ObjectMapper();
            mapper.registerModule(new JavaTimeModule());
            // Enable default typing for polymorphic deserialization
            mapper.activateDefaultTyping(
                    mapper.getPolymorphicTypeValidator(),
                    ObjectMapper.DefaultTyping.NON_FINAL,
                    JsonTypeInfo.As.PROPERTY
            );
            return mapper;
        }

        @Bean
        public ProducerFactory<String, TestEvent> producerFactory(EmbeddedKafkaBroker embeddedKafka, ObjectMapper kafkaObjectMapper) {
            Map<String, Object> props = new HashMap<>();
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, embeddedKafka.getBrokersAsString());
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
            props.put(JsonSerializer.ADD_TYPE_INFO_HEADERS, false);

            DefaultKafkaProducerFactory<String, TestEvent> factory = new DefaultKafkaProducerFactory<>(props);
            // Use the shared ObjectMapper
            JsonSerializer<TestEvent> serializer = new JsonSerializer<>(kafkaObjectMapper);
            factory.setValueSerializer(serializer);
            return factory;
        }

        @Bean
        public KafkaTemplate<String, TestEvent> kafkaTemplate(ProducerFactory<String, TestEvent> producerFactory) {
            return new KafkaTemplate<>(producerFactory);
        }

        @Bean
        public ConsumerFactory<String, TestEvent> testEventConsumerFactory(
                EmbeddedKafkaBroker embeddedKafka,
                ObjectMapper kafkaObjectMapper) {
            Map<String, Object> props = new HashMap<>();
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, embeddedKafka.getBrokersAsString());
            props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group");
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);

            JsonDeserializer<TestEvent> jsonDeserializer = new JsonDeserializer<>(TestEvent.class, kafkaObjectMapper);
            jsonDeserializer.addTrustedPackages("*");
            jsonDeserializer.setUseTypeHeaders(false);

            return new DefaultKafkaConsumerFactory<>(
                    props,
                    new StringDeserializer(),
                    jsonDeserializer
            );
        }

        @Bean(name = "kafkaListenerContainerFactory")
        public ConcurrentKafkaListenerContainerFactory<String, TestEvent> kafkaListenerContainerFactory(
                ConsumerFactory<String, TestEvent> testEventConsumerFactory) {
            ConcurrentKafkaListenerContainerFactory<String, TestEvent> factory =
                    new ConcurrentKafkaListenerContainerFactory<>();
            factory.setConsumerFactory(testEventConsumerFactory);
            // Disable auto-commit to have more control during retries
            factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
            return factory;
        }

        @Bean(name = "defaultFactory")
        public ConcurrentKafkaListenerContainerFactory<String, Object> defaultFactory(
                EmbeddedKafkaBroker embeddedKafka,
                ObjectMapper kafkaObjectMapper) {

            Map<String, Object> props = new HashMap<>();
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, embeddedKafka.getBrokersAsString());
            props.put(ConsumerConfig.GROUP_ID_CONFIG, "default-group");
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
            return factory;
        }

        @Bean
        public ConsumerFactory<String, EventWrapper<?>> dlqConsumerFactory(
                EmbeddedKafkaBroker embeddedKafka,
                ObjectMapper kafkaObjectMapper) {
            Map<String, Object> props = new HashMap<>();
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, embeddedKafka.getBrokersAsString());
            props.put(ConsumerConfig.GROUP_ID_CONFIG, "dlq-test-group");
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);

            // Use the shared ObjectMapper with polymorphic type handling
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

        @Bean(name = "dlqKafkaListenerContainerFactory")
        public ConcurrentKafkaListenerContainerFactory<String, EventWrapper<?>> dlqKafkaListenerContainerFactory(
                ConsumerFactory<String, EventWrapper<?>> dlqConsumerFactory) {
            ConcurrentKafkaListenerContainerFactory<String, EventWrapper<?>> factory =
                    new ConcurrentKafkaListenerContainerFactory<>();
            factory.setConsumerFactory(dlqConsumerFactory);
            return factory;
        }

        @Bean(name = "defaultKafkaTemplate")
        public KafkaTemplate<String, Object> defaultKafkaTemplate(
                EmbeddedKafkaBroker embeddedKafka,
                ObjectMapper kafkaObjectMapper) {
            Map<String, Object> props = new HashMap<>();
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, embeddedKafka.getBrokersAsString());
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
            props.put(JsonSerializer.ADD_TYPE_INFO_HEADERS, false);

            DefaultKafkaProducerFactory<String, Object> factory = new DefaultKafkaProducerFactory<>(props);
            // Use the shared ObjectMapper for consistent serialization
            JsonSerializer<Object> serializer = new JsonSerializer<>(kafkaObjectMapper);
            factory.setValueSerializer(serializer);

            return new KafkaTemplate<>(factory);
        }
    }
}