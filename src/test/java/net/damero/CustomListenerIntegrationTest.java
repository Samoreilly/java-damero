package net.damero;

import com.fasterxml.jackson.databind.ObjectMapper;
import net.damero.Kafka.Config.CustomKafkaAutoConfiguration;
import net.damero.Kafka.CustomObject.EventWrapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.*;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest(classes = CustomKafkaListenerIntegrationTest.TestConfig.class)
@EmbeddedKafka(
        partitions = 1,
        topics = {"test-topic", "test-topic-retry", "test-dlq"},
        brokerProperties = {
                "listeners=PLAINTEXT://localhost:0"
        }
)
@AutoConfigureMockMvc
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class CustomKafkaListenerIntegrationTest {

    @Autowired
    private EmbeddedKafkaBroker embeddedKafka;

    @Autowired
    private KafkaTemplate<?, Object> kafkaTemplate;

    @Autowired
    private TestKafkaListener testKafkaListener;

    @Autowired
    private DLQMessageCollector dlqCollector;

    @Autowired
    private MockMvc mockMvc;

    @BeforeEach
    void setUp() {
        testKafkaListener.reset();
        dlqCollector.reset();
    }

    @Test
    void testSuccessfulMessageProcessing() {
        // Given: An event that should succeed
        TestEvent event = new TestEvent("success-1", "test-data", false);

        // When: Sending to Kafka
        kafkaTemplate.send("test-topic", event);

        // Then: Event should be processed successfully
        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            assertEquals(1, testKafkaListener.getSuccessfulEvents().size());
            assertEquals(0, testKafkaListener.getFailedEvents().size());
            assertEquals(1, testKafkaListener.getAttemptCount("success-1"));
        });
    }

    @Test
    void testEventRetriesAndSendsToDLQ() {
        // Given: An event that always fails
        TestEvent event = new TestEvent("fail-1", "test-data", true);

        // When: Sending to Kafka
        kafkaTemplate.send("test-topic", event);

        // Then: Should retry 3 times and send to DLQ
        await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> {
            // Verify attempts
            assertEquals(3, testKafkaListener.getAttemptCount("fail-1"));

            // Verify DLQ message received
            EventWrapper<?> dlqMessage = dlqCollector.getLastMessage();
            assertNotNull(dlqMessage);

            // Verify the event in DLQ is NOT double-wrapped
            assertFalse(dlqMessage.getEvent() instanceof EventWrapper,
                    "Event should not be wrapped twice!");

            // Verify it's the original event
            assertTrue(dlqMessage.getEvent() instanceof TestEvent);
            TestEvent dlqEvent = (TestEvent) dlqMessage.getEvent();
            assertEquals("fail-1", dlqEvent.getId());

            // Verify metadata
            assertNotNull(dlqMessage.getMetadata());
            assertEquals(3, dlqMessage.getMetadata().getAttempts());
        });
    }

    @Test
    void testEventSucceedsAfterRetry() {
        // Given: An event that fails initially but succeeds later
        TestEvent event = new TestEvent("retry-success-1", "test-data", true);

        // When: Sending to Kafka
        kafkaTemplate.send("test-topic", event);

        // Wait for first failure
        await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
            assertTrue(testKafkaListener.getAttemptCount("retry-success-1") >= 1);
        });

        // Then: Manually mark as should succeed (simulating transient failure)
        event.setShouldFail(false);

        // Should eventually succeed
        await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> {
            assertTrue(testKafkaListener.getSuccessfulEvents().stream()
                    .anyMatch(e -> e.getId().equals("retry-success-1")));
        });
    }

    @Test
    void testMultipleEventsProcessedIndependently() {
        // Given: Multiple events with different behaviors
        TestEvent success = new TestEvent("multi-success", "data", false);
        TestEvent fail = new TestEvent("multi-fail", "data", true);

        // When: Sending both
        kafkaTemplate.send("test-topic", success);
        kafkaTemplate.send("test-topic", fail);

        // Then: Success should process, failure should go to DLQ
        await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> {
            // Success event processed once
            assertEquals(1, testKafkaListener.getAttemptCount("multi-success"));
            assertTrue(testKafkaListener.getSuccessfulEvents().stream()
                    .anyMatch(e -> e.getId().equals("multi-success")));

            // Fail event retried 4 times
            assertEquals(3, testKafkaListener.getAttemptCount("multi-fail"));

            // Verify DLQ received the failed event
            EventWrapper<?> dlqMessage = dlqCollector.getLastMessage();
            assertNotNull(dlqMessage);
            assertEquals("multi-fail", ((TestEvent) dlqMessage.getEvent()).getId());
        });
    }

    @Test
    void testMetadataTracksFailureHistory() {
        // Given: A failing event
        TestEvent event = new TestEvent("metadata-test", "data", true);

        // When: Sending to Kafka
        kafkaTemplate.send("test-topic", event);

        // Then: Metadata should track all failures
        await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> {
            EventWrapper<?> dlqMessage = dlqCollector.getLastMessage();
            assertNotNull(dlqMessage);

            var metadata = dlqMessage.getMetadata();
            assertNotNull(metadata.getFirstFailureDateTime());
            assertNotNull(metadata.getLastFailureDateTime());
            assertNotNull(metadata.getFirstFailureException());
            assertNotNull(metadata.getLastFailureException());
            assertEquals(3, metadata.getAttempts());
            assertEquals("test-dlq", metadata.getDlqTopic());
        });
    }

    @Test
    void testDLQEndToEndFlow() throws Exception {
        // Use the same ID as in the assertion
        TestEvent event = new TestEvent("end-to-end-dlq", "data", true);
        EventWrapper<?> wrapper = new EventWrapper<>(event, null, null);

        // Reset collector to avoid old messages
        dlqCollector.reset();

        // Send the wrapper directly to DLQ
        kafkaTemplate.send("test-dlq", wrapper);
        kafkaTemplate.flush();

        // Wait until DLQMessageCollector receives the message
        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            EventWrapper<?> dlqMessage = dlqCollector.getLastMessage();
            assertNotNull(dlqMessage, "Message should reach DLQ");
            assertEquals("end-to-end-dlq", ((TestEvent) dlqMessage.getEvent()).getId());
        });

        // Call API endpoint that reads from DLQ
        MvcResult result = mockMvc.perform(get("/dlq"))
                .andExpect(status().isOk())
                .andReturn();

        String responseBody = result.getResponse().getContentAsString();

        assertTrue(responseBody.contains("end-to-end-dlq"),
                "API response should contain the DLQ event ID");
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
        public ProducerFactory<String, TestEvent> testEventProducerFactory(ObjectMapper kafkaObjectMapper) {
            Map<String, Object> props = new HashMap<>(
                    org.springframework.kafka.test.utils.KafkaTestUtils.producerProps(embeddedKafka)
            );
            props.put(org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringSerializer.class);
            props.put(org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, org.springframework.kafka.support.serializer.JsonSerializer.class);

            DefaultKafkaProducerFactory<String, TestEvent> factory = new DefaultKafkaProducerFactory<>(props);
            org.springframework.kafka.support.serializer.JsonSerializer<TestEvent> serializer = new org.springframework.kafka.support.serializer.JsonSerializer<>(kafkaObjectMapper);
            serializer.setAddTypeInfo(true);
            factory.setValueSerializer(serializer);
            return factory;
        }

        // Use the auto-configured kafkaListenerContainerFactory from CustomKafkaAutoConfiguration

        @Bean
        public KafkaTemplate<String, TestEvent> kafkaTemplate(
                ProducerFactory<String, TestEvent> testEventProducerFactory) {
            return new KafkaTemplate<>(testEventProducerFactory);
        }

        @Bean(name = "kafkaListenerContainerFactory")
        public ConcurrentKafkaListenerContainerFactory<String, Object> testKafkaListenerContainerFactory(ObjectMapper kafkaObjectMapper) {
            Map<String, Object> props = new HashMap<>();
            props.put(org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, embeddedKafka.getBrokersAsString());
            props.put(org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG, "test-group");
            props.put(org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

            org.springframework.kafka.support.serializer.JsonDeserializer<Object> jsonDeserializer =
                    new org.springframework.kafka.support.serializer.JsonDeserializer<>(kafkaObjectMapper);
            jsonDeserializer.addTrustedPackages("*");
            jsonDeserializer.setUseTypeHeaders(true);

            ConsumerFactory<String, Object> consumerFactory = new DefaultKafkaConsumerFactory<>(
                    props,
                    new org.apache.kafka.common.serialization.StringDeserializer(),
                    jsonDeserializer
            );

            ConcurrentKafkaListenerContainerFactory<String, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
            factory.setConsumerFactory(consumerFactory);
            factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
            factory.setCommonErrorHandler(null);
            return factory;
        }

        // Override the library's default producer so retries/DLQ send to embedded broker
        @Bean(name = "defaultKafkaTemplate")
        public KafkaTemplate<String, Object> defaultKafkaTemplate(ObjectMapper kafkaObjectMapper) {
            Map<String, Object> props = new HashMap<>(KafkaTestUtils.producerProps(embeddedKafka));
            props.put(org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringSerializer.class);
            props.put(org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, org.springframework.kafka.support.serializer.JsonSerializer.class);

            DefaultKafkaProducerFactory<String, Object> factory = new DefaultKafkaProducerFactory<>(props);
            org.springframework.kafka.support.serializer.JsonSerializer<Object> serializer = new org.springframework.kafka.support.serializer.JsonSerializer<>(kafkaObjectMapper);
            serializer.setAddTypeInfo(true);
            factory.setValueSerializer(serializer);
            return new KafkaTemplate<>(factory);
        }

        // Override DLQ container factory to use embedded broker
        @Bean(name = "dlqKafkaListenerContainerFactory")
        public ConcurrentKafkaListenerContainerFactory<String, EventWrapper<?>> dlqKafkaListenerContainerFactory(ObjectMapper kafkaObjectMapper) {
            Map<String, Object> props = new HashMap<>();
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, embeddedKafka.getBrokersAsString());
            props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-dlq-group");
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

            JsonDeserializer<EventWrapper<?>> deserializer = new JsonDeserializer<>(EventWrapper.class, kafkaObjectMapper);
            deserializer.addTrustedPackages("*");
            deserializer.setUseTypeHeaders(true);

            ConsumerFactory<String, EventWrapper<?>> consumerFactory = new DefaultKafkaConsumerFactory<>(
                    props,
                    new StringDeserializer(),
                    deserializer
            );

            ConcurrentKafkaListenerContainerFactory<String, EventWrapper<?>> factory = new ConcurrentKafkaListenerContainerFactory<>();
            factory.setConsumerFactory(consumerFactory);
            factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
            return factory;
        }
    }

}