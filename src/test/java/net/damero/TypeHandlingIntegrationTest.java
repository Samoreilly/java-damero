package net.damero;

import com.fasterxml.jackson.databind.ObjectMapper;
import net.damero.Kafka.Annotations.DameroKafkaListener;
import net.damero.Kafka.Config.CustomKafkaAutoConfiguration;
import net.damero.Kafka.CustomObject.EventWrapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.*;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.stereotype.Component;
import org.springframework.test.annotation.DirtiesContext;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.List;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest(classes = TypeHandlingIntegrationTest.TestConfig.class)
@EmbeddedKafka(partitions = 1, topics = { "double-topic", "string-topic", "object-topic",
        "dlq-topic" }, brokerProperties = { "listeners=PLAINTEXT://localhost:0" })
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
public class TypeHandlingIntegrationTest {

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    @Autowired
    private DoubleListener doubleListener;

    @Autowired
    private StringListener stringListener;

    @Autowired
    private DLQListener dlqListener;

    @BeforeEach
    void setUp() {
        doubleListener.reset();
        stringListener.reset();
        dlqListener.reset();
    }

    @Test
    @DisplayName("Should handle Double type and retries correctly")
    void testDoubleTypeHandling() {
        // Sending a double that will fail first time
        kafkaTemplate.send("double-topic", 123.45);
        kafkaTemplate.flush();

        // Should be received first time (failure) and then retried
        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            assertTrue(doubleListener.getCallCount() >= 2, "Should be called at least twice (original + retry)");
        });
    }

    @Test
    @DisplayName("Should handle String type and deduplication correctly")
    void testStringTypeHandling() {
        // Sending same string twice - if deduplication is on, second should be ignored
        // But for strings, we shouldn't deduplicate by value unless explicitly asked
        kafkaTemplate.send("string-topic", "test-message");
        kafkaTemplate.send("string-topic", "test-message");
        kafkaTemplate.flush();

        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            assertEquals(2, stringListener.getCallCount(),
                    "Should receive both messages if deduplication is handled correctly");
        });
    }

    @Test
    @DisplayName("Should handle DLQ deserialization correctly for EventWrapper")
    void testDLQDeserialization() {
        // Sending message that will fail and go to DLQ
        kafkaTemplate.send("double-topic", 999.99); // 999.99 triggers direct DLQ in our test listener
        kafkaTemplate.flush();

        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            assertEquals(1, dlqListener.getReceivedWrappers().size(),
                    "Message should reach DLQ and be deserializable as EventWrapper");
        });
    }

    @Component
    static class DoubleListener {
        private final AtomicInteger callCount = new AtomicInteger(0);

        @DameroKafkaListener(topic = "double-topic", dlqTopic = "dlq-topic", maxAttempts = 2, delay = 100)
        @KafkaListener(topics = "double-topic", groupId = "double-group")
        public void listen(Double value, Acknowledgment ack) {
            int count = callCount.incrementAndGet();
            if (value == 999.99) {
                throw new RuntimeException("Direct to DLQ");
            }
            if (count == 1) {
                throw new RuntimeException("Fail first time");
            }
            ack.acknowledge();
        }

        public int getCallCount() {
            return callCount.get();
        }

        public void reset() {
            callCount.set(0);
        }
    }

    @Component
    static class StringListener {
        private final AtomicInteger callCount = new AtomicInteger(0);

        @DameroKafkaListener(topic = "string-topic", deDuplication = true) // Deduplication enabled
        @KafkaListener(topics = "string-topic", groupId = "string-group")
        public void listenString(String data, ConsumerRecord<String, String> record, Acknowledgment ack) {
            callCount.incrementAndGet();
            ack.acknowledge();
        }

        public int getCallCount() {
            return callCount.get();
        }

        public void reset() {
            callCount.set(0);
        }
    }

    @Component
    static class DLQListener {
        private final List<EventWrapper<?>> receivedWrappers = new CopyOnWriteArrayList<>();

        @KafkaListener(topics = "dlq-topic", groupId = "dlq-group")
        public void listen(EventWrapper<?> wrapper, Acknowledgment ack) {
            receivedWrappers.add(wrapper);
            ack.acknowledge();
        }

        public List<EventWrapper<?>> getReceivedWrappers() {
            return receivedWrappers;
        }

        public void reset() {
            receivedWrappers.clear();
        }
    }

    @Configuration
    @EnableKafka
    @EnableAspectJAutoProxy
    @ComponentScan(basePackages = "net.damero")
    @Import(CustomKafkaAutoConfiguration.class)
    static class TestConfig {
        @Autowired
        private EmbeddedKafkaBroker embeddedKafka;

        @Bean
        public ProducerFactory<String, Object> producerFactory(ObjectMapper kafkaObjectMapper) {
            Map<String, Object> props = new HashMap<>(KafkaTestUtils.producerProps(embeddedKafka));
            props.put(org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                    org.apache.kafka.common.serialization.StringSerializer.class);
            props.put(org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                    org.springframework.kafka.support.serializer.JsonSerializer.class);

            DefaultKafkaProducerFactory<String, Object> factory = new DefaultKafkaProducerFactory<>(props);
            org.springframework.kafka.support.serializer.JsonSerializer<Object> serializer = new org.springframework.kafka.support.serializer.JsonSerializer<>(
                    kafkaObjectMapper);
            serializer.setAddTypeInfo(true);
            factory.setValueSerializer(serializer);
            return factory;
        }

        @Bean
        public KafkaTemplate<String, Object> kafkaTemplate(ProducerFactory<String, Object> producerFactory) {
            return new KafkaTemplate<>(producerFactory);
        }

        @Bean
        public ConcurrentKafkaListenerContainerFactory<String, Object> kafkaListenerContainerFactory(
                ObjectMapper kafkaObjectMapper) {
            Map<String, Object> props = new HashMap<>();
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, embeddedKafka.getBrokersAsString());
            props.put(ConsumerConfig.GROUP_ID_CONFIG, "type-test-group");
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

            JsonDeserializer<Object> jsonDeserializer = new JsonDeserializer<>(kafkaObjectMapper);
            jsonDeserializer.addTrustedPackages("*");
            jsonDeserializer.setUseTypeHeaders(true);

            ConsumerFactory<String, Object> consumerFactory = new DefaultKafkaConsumerFactory<>(
                    props,
                    new org.apache.kafka.common.serialization.StringDeserializer(),
                    jsonDeserializer);

            ConcurrentKafkaListenerContainerFactory<String, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
            factory.setConsumerFactory(consumerFactory);
            factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
            return factory;
        }

        @Bean(name = "defaultKafkaTemplate")
        public KafkaTemplate<String, Object> defaultKafkaTemplate(ProducerFactory<String, Object> producerFactory) {
            return new KafkaTemplate<>(producerFactory);
        }
    }
}
