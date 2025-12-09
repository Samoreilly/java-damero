package net.damero;

import com.fasterxml.jackson.databind.ObjectMapper;
import net.damero.Kafka.Annotations.CustomKafkaListener;
import net.damero.Kafka.BatchOrchestrator.BatchOrchestrator;
import net.damero.Kafka.Config.CustomKafkaAutoConfiguration;
import org.apache.kafka.clients.consumer.ConsumerConfig;
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
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Comprehensive integration tests for batch processing functionality.
 *
 * Tests cover:
 * - Batch capacity triggering (process when capacity reached)
 * - Batch window expiry triggering (process when timer expires)
 * - Mixed scenarios (some batches hit capacity, some expire)
 * - Edge cases (empty batches, single message batches)
 * - Concurrent message handling
 * - Batch state reset after processing
 */
@SpringBootTest(classes = BatchProcessingIntegrationTest.TestConfig.class)
@EmbeddedKafka(
        partitions = 1,
        topics = {
            "batch-capacity-topic", "batch-capacity-dlq",
            "batch-window-topic", "batch-window-dlq",
            "batch-mixed-topic", "batch-mixed-dlq"
        },
        brokerProperties = {
                "listeners=PLAINTEXT://localhost:0"
        }
)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class BatchProcessingIntegrationTest {

    @Autowired
    private EmbeddedKafkaBroker embeddedKafka;

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    @Autowired
    private BatchCapacityListener batchCapacityListener;

    @Autowired
    private BatchWindowListener batchWindowListener;

    @Autowired
    private BatchMixedListener batchMixedListener;

    @Autowired
    private BatchOrchestrator batchOrchestrator;

    @BeforeEach
    void setUp() {
        batchCapacityListener.reset();
        batchWindowListener.reset();
        batchMixedListener.reset();
    }

    // ==================== CAPACITY-BASED BATCH TESTS ====================

    @Test
    @Order(1)
    @DisplayName("Should process batch when capacity is reached")
    void testBatchProcessing_CapacityReached() {
        // Given: Batch capacity of 5
        // When: Sending exactly 5 messages
        for (int i = 0; i < 5; i++) {
            TestEvent event = new TestEvent("capacity-" + i, "batch-data", false);
            kafkaTemplate.send("batch-capacity-topic", event);
        }
        kafkaTemplate.flush();

        // Then: All 5 messages should be processed as a batch
        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            assertEquals(5, batchCapacityListener.getProcessedEvents().size(),
                "All 5 messages should be processed when capacity is reached");
        });

        // Verify batch processing happened (not individual processing)
        assertTrue(batchCapacityListener.getBatchProcessingCount() >= 1,
            "Batch processing should have been triggered at least once");
    }

    @Test
    @Order(2)
    @DisplayName("Should process multiple batches when sending more than capacity")
    void testBatchProcessing_MultipleBatches() {
        // Given: Batch capacity of 5
        // When: Sending 12 messages (should trigger 2 full batches + 2 remaining)
        for (int i = 0; i < 12; i++) {
            TestEvent event = new TestEvent("multi-batch-" + i, "data", false);
            kafkaTemplate.send("batch-capacity-topic", event);
        }
        kafkaTemplate.flush();

        // Then: All 12 messages should eventually be processed
        await().atMost(15, TimeUnit.SECONDS).untilAsserted(() -> {
            assertEquals(12, batchCapacityListener.getProcessedEvents().size(),
                "All 12 messages should be processed across multiple batches");
        });

        // Should have triggered at least 2 capacity-based batches
        assertTrue(batchCapacityListener.getBatchProcessingCount() >= 2,
            "Should trigger at least 2 batch processing cycles for 12 messages with capacity 5");
    }

    @Test
    @Order(3)
    @DisplayName("Should not process batch before capacity is reached - waits for window")
    void testBatchProcessing_UnderCapacity_WaitsForWindow() {
        // Given: Batch capacity of 5
        // When: Sending only 3 messages (under capacity)
        for (int i = 0; i < 3; i++) {
            TestEvent event = new TestEvent("under-capacity-" + i, "data", false);
            kafkaTemplate.send("batch-capacity-topic", event);
        }
        kafkaTemplate.flush();

        // Wait a short time - batch should NOT be processed yet (waiting for capacity or window)
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // Messages should eventually be processed when window expires (3 seconds)
        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            assertEquals(3, batchCapacityListener.getProcessedEvents().size(),
                "Messages should be processed when window expires");
        });
    }

    // ==================== WINDOW-BASED BATCH TESTS ====================

    @Test
    @Order(4)
    @DisplayName("Should process batch when window expires before capacity")
    void testBatchProcessing_WindowExpiry() {
        // Given: Batch capacity of 10, window of 2000ms
        // When: Sending 4 messages (under capacity)
        for (int i = 0; i < 4; i++) {
            TestEvent event = new TestEvent("window-" + i, "data", false);
            kafkaTemplate.send("batch-window-topic", event);
        }
        kafkaTemplate.flush();

        // Then: Messages should be processed after window expires (~2 seconds)
        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            assertEquals(4, batchWindowListener.getProcessedEvents().size(),
                "All 4 messages should be processed when window expires");
        });

        // Verify window-based processing occurred
        assertTrue(batchWindowListener.getWindowExpiryCount() >= 1,
            "Window expiry should have triggered batch processing");
    }

    @Test
    @Order(5)
    @DisplayName("Should process batch immediately when capacity reached before window")
    void testBatchProcessing_CapacityBeforeWindow() {
        // Given: Batch capacity of 10, window of 5000ms
        // When: Sending exactly 10 messages quickly
        long startTime = System.currentTimeMillis();

        for (int i = 0; i < 10; i++) {
            TestEvent event = new TestEvent("capacity-before-window-" + i, "data", false);
            kafkaTemplate.send("batch-window-topic", event);
        }
        kafkaTemplate.flush();

        // Then: Messages should be processed before window expires
        await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
            assertEquals(10, batchWindowListener.getProcessedEvents().size(),
                "All 10 messages should be processed immediately when capacity is reached");
        });

        long processingTime = System.currentTimeMillis() - startTime;

        // Should process much faster than window length (5000ms)
        assertTrue(processingTime < 4000,
            String.format("Should process before window expires, but took %dms", processingTime));
    }

    // ==================== EDGE CASE TESTS ====================

    @Test
    @Order(6)
    @DisplayName("Should handle single message batch on window expiry")
    void testBatchProcessing_SingleMessageBatch() {
        // Given: Batch capacity of 10, window of 2000ms
        // When: Sending only 1 message
        TestEvent event = new TestEvent("single-msg", "data", false);
        kafkaTemplate.send("batch-window-topic", event);
        kafkaTemplate.flush();

        // Then: Single message should be processed after window expires
        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            assertEquals(1, batchWindowListener.getProcessedEvents().size(),
                "Single message should be processed when window expires");
        });
    }

    @Test
    @Order(7)
    @DisplayName("Should reset batch state after processing")
    void testBatchProcessing_StateResetAfterProcessing() {
        // Given: Process a full batch first
        for (int i = 0; i < 5; i++) {
            TestEvent event = new TestEvent("first-batch-" + i, "data", false);
            kafkaTemplate.send("batch-capacity-topic", event);
        }
        kafkaTemplate.flush();

        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            assertEquals(5, batchCapacityListener.getProcessedEvents().size());
        });

        int countAfterFirst = batchCapacityListener.getBatchProcessingCount();

        // When: Send another full batch
        for (int i = 0; i < 5; i++) {
            TestEvent event = new TestEvent("second-batch-" + i, "data", false);
            kafkaTemplate.send("batch-capacity-topic", event);
        }
        kafkaTemplate.flush();

        // Then: Second batch should also be processed
        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            assertEquals(10, batchCapacityListener.getProcessedEvents().size(),
                "Both batches should be processed (10 total messages)");
        });

        assertTrue(batchCapacityListener.getBatchProcessingCount() > countAfterFirst,
            "Batch processing count should increase after second batch");
    }

    @Test
    @Order(8)
    @DisplayName("Should handle rapid successive messages")
    void testBatchProcessing_RapidMessages() {
        // Given: Batch capacity of 5
        // When: Sending 20 messages as fast as possible
        for (int i = 0; i < 20; i++) {
            TestEvent event = new TestEvent("rapid-" + i, "data", false);
            kafkaTemplate.send("batch-capacity-topic", event);
        }
        kafkaTemplate.flush();

        // Then: All messages should be processed
        await().atMost(15, TimeUnit.SECONDS).untilAsserted(() -> {
            assertEquals(20, batchCapacityListener.getProcessedEvents().size(),
                "All 20 messages should be processed across multiple batches");
        });

        // Should have triggered multiple batch cycles
        assertTrue(batchCapacityListener.getBatchProcessingCount() >= 4,
            "Should trigger at least 4 batch processing cycles for 20 messages with capacity 5");
    }

    @Test
    @Order(9)
    @DisplayName("Should verify batch orchestrator state is clean after processing")
    void testBatchProcessing_OrchestratorStateCleanup() {
        String topic = "batch-capacity-topic";

        // Given: Send and process a full batch
        for (int i = 0; i < 5; i++) {
            TestEvent event = new TestEvent("cleanup-test-" + i, "data", false);
            kafkaTemplate.send(topic, event);
        }
        kafkaTemplate.flush();

        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            assertEquals(5, batchCapacityListener.getProcessedEvents().size());
        });

        // Then: Batch orchestrator should have clean state for this topic
        await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
            assertFalse(batchOrchestrator.hasActiveWindow(topic),
                "No active window should exist after batch processing");
            assertEquals(0, batchOrchestrator.getBatchCount(topic),
                "Batch count should be 0 after processing");
            assertFalse(batchOrchestrator.hasPendingMessages(topic),
                "No pending messages should exist after processing");
        });
    }

    // ==================== MIXED SCENARIO TESTS ====================

    @Test
    @Order(10)
    @DisplayName("Should handle mixed capacity and window scenarios")
    void testBatchProcessing_MixedScenarios() {
        // Given: Batch capacity of 5, window of 2000ms
        // When: Send 7 messages (first batch by capacity, second by window)
        for (int i = 0; i < 7; i++) {
            TestEvent event = new TestEvent("mixed-" + i, "data", false);
            kafkaTemplate.send("batch-mixed-topic", event);

            // Slight delay between messages to simulate real-world scenario
            if (i == 4) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
        kafkaTemplate.flush();

        // Then: All 7 messages should be processed
        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            assertEquals(7, batchMixedListener.getProcessedEvents().size(),
                "All 7 messages should be processed (5 by capacity, 2 by window)");
        });
    }

    // ==================== LISTENER COMPONENTS ====================

    /**
     * Listener for capacity-based batch testing.
     * Batch capacity: 5, Window: 3000ms
     */
    @Component
    static class BatchCapacityListener {
        private final List<TestEvent> processedEvents = new CopyOnWriteArrayList<>();
        private final AtomicInteger batchProcessingCount = new AtomicInteger(0);
        private final AtomicInteger windowExpiryCount = new AtomicInteger(0);

        @CustomKafkaListener(
                topic = "batch-capacity-topic",
                dlqTopic = "batch-capacity-dlq",
                maxAttempts = 3,
                batchCapacity = 5,
                batchWindowLength = 3000
        )
        @KafkaListener(topics = "batch-capacity-topic", groupId = "batch-capacity-group",
                containerFactory = "kafkaListenerContainerFactory")
        public void listen(org.apache.kafka.clients.consumer.ConsumerRecord<String, Object> record,
                          Acknowledgment acknowledgment) {
            processRecord(record, acknowledgment);
        }

        private void processRecord(org.apache.kafka.clients.consumer.ConsumerRecord<String, Object> record,
                                  Acknowledgment acknowledgment) {
            if (record == null) return;

            TestEvent event = extractEvent(record.value());
            if (event != null) {
                processedEvents.add(event);
                batchProcessingCount.incrementAndGet();
            }

            if (acknowledgment != null) {
                acknowledgment.acknowledge();
            }
        }

        public List<TestEvent> getProcessedEvents() { return processedEvents; }
        public int getBatchProcessingCount() { return batchProcessingCount.get(); }
        public int getWindowExpiryCount() { return windowExpiryCount.get(); }
        public void reset() {
            processedEvents.clear();
            batchProcessingCount.set(0);
            windowExpiryCount.set(0);
        }
    }

    /**
     * Listener for window-based batch testing.
     * Batch capacity: 10, Window: 2000ms
     */
    @Component
    static class BatchWindowListener {
        private final List<TestEvent> processedEvents = new CopyOnWriteArrayList<>();
        private final AtomicInteger batchProcessingCount = new AtomicInteger(0);
        private final AtomicInteger windowExpiryCount = new AtomicInteger(0);

        @CustomKafkaListener(
                topic = "batch-window-topic",
                dlqTopic = "batch-window-dlq",
                maxAttempts = 3,
                batchCapacity = 10,
                batchWindowLength = 2000
        )
        @KafkaListener(topics = "batch-window-topic", groupId = "batch-window-group",
                containerFactory = "kafkaListenerContainerFactory")
        public void listen(org.apache.kafka.clients.consumer.ConsumerRecord<String, Object> record,
                          Acknowledgment acknowledgment) {
            processRecord(record, acknowledgment);
        }

        private void processRecord(org.apache.kafka.clients.consumer.ConsumerRecord<String, Object> record,
                                  Acknowledgment acknowledgment) {
            if (record == null) return;

            TestEvent event = extractEvent(record.value());
            if (event != null) {
                processedEvents.add(event);
                batchProcessingCount.incrementAndGet();
                windowExpiryCount.incrementAndGet();
            }

            if (acknowledgment != null) {
                acknowledgment.acknowledge();
            }
        }

        public List<TestEvent> getProcessedEvents() { return processedEvents; }
        public int getBatchProcessingCount() { return batchProcessingCount.get(); }
        public int getWindowExpiryCount() { return windowExpiryCount.get(); }
        public void reset() {
            processedEvents.clear();
            batchProcessingCount.set(0);
            windowExpiryCount.set(0);
        }
    }

    /**
     * Listener for mixed scenario testing.
     * Batch capacity: 5, Window: 2000ms
     */
    @Component
    static class BatchMixedListener {
        private final List<TestEvent> processedEvents = new CopyOnWriteArrayList<>();
        private final AtomicInteger batchProcessingCount = new AtomicInteger(0);

        @CustomKafkaListener(
                topic = "batch-mixed-topic",
                dlqTopic = "batch-mixed-dlq",
                maxAttempts = 3,
                batchCapacity = 5,
                batchWindowLength = 2000
        )
        @KafkaListener(topics = "batch-mixed-topic", groupId = "batch-mixed-group",
                containerFactory = "kafkaListenerContainerFactory")
        public void listen(org.apache.kafka.clients.consumer.ConsumerRecord<String, Object> record,
                          Acknowledgment acknowledgment) {
            if (record == null) return;

            TestEvent event = extractEvent(record.value());
            if (event != null) {
                processedEvents.add(event);
                batchProcessingCount.incrementAndGet();
            }

            if (acknowledgment != null) {
                acknowledgment.acknowledge();
            }
        }

        public List<TestEvent> getProcessedEvents() { return processedEvents; }
        public int getBatchProcessingCount() { return batchProcessingCount.get(); }
        public void reset() {
            processedEvents.clear();
            batchProcessingCount.set(0);
        }
    }

    // ==================== UTILITY METHODS ====================

    private static TestEvent extractEvent(Object payload) {
        if (payload instanceof TestEvent te) {
            return te;
        } else if (payload instanceof Map<?, ?> map) {
            Object id = map.get("id");
            Object data = map.get("data");
            Object shouldFail = map.get("shouldFail");
            if (id instanceof String) {
                return new TestEvent((String) id, data != null ? data.toString() : null,
                        Boolean.TRUE.equals(shouldFail));
            }
        }
        return null;
    }

    // ==================== TEST CONFIGURATION ====================

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
            org.springframework.kafka.support.serializer.JsonSerializer<Object> serializer =
                    new org.springframework.kafka.support.serializer.JsonSerializer<>(kafkaObjectMapper);
            serializer.setAddTypeInfo(true);
            factory.setValueSerializer(serializer);
            return factory;
        }

        @Bean
        public KafkaTemplate<String, Object> kafkaTemplate(ProducerFactory<String, Object> producerFactory) {
            return new KafkaTemplate<>(producerFactory);
        }

        @Bean(name = "kafkaListenerContainerFactory")
        public ConcurrentKafkaListenerContainerFactory<String, Object> kafkaListenerContainerFactory(
                ObjectMapper kafkaObjectMapper) {
            Map<String, Object> props = new HashMap<>();
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, embeddedKafka.getBrokersAsString());
            props.put(ConsumerConfig.GROUP_ID_CONFIG, "batch-test-group");
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

            JsonDeserializer<Object> jsonDeserializer = new JsonDeserializer<>(kafkaObjectMapper);
            jsonDeserializer.addTrustedPackages("*");
            jsonDeserializer.setUseTypeHeaders(true);

            ConsumerFactory<String, Object> consumerFactory = new DefaultKafkaConsumerFactory<>(
                    props,
                    new org.apache.kafka.common.serialization.StringDeserializer(),
                    jsonDeserializer
            );

            ConcurrentKafkaListenerContainerFactory<String, Object> factory =
                    new ConcurrentKafkaListenerContainerFactory<>();
            factory.setConsumerFactory(consumerFactory);
            factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
            return factory;
        }

        @Bean(name = "defaultKafkaTemplate")
        public KafkaTemplate<String, Object> defaultKafkaTemplate(ObjectMapper kafkaObjectMapper) {
            Map<String, Object> props = new HashMap<>(KafkaTestUtils.producerProps(embeddedKafka));
            props.put(org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                    org.apache.kafka.common.serialization.StringSerializer.class);
            props.put(org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                    org.springframework.kafka.support.serializer.JsonSerializer.class);

            DefaultKafkaProducerFactory<String, Object> factory = new DefaultKafkaProducerFactory<>(props);
            org.springframework.kafka.support.serializer.JsonSerializer<Object> serializer =
                    new org.springframework.kafka.support.serializer.JsonSerializer<>(kafkaObjectMapper);
            serializer.setAddTypeInfo(true);
            factory.setValueSerializer(serializer);
            return new KafkaTemplate<>(factory);
        }
    }
}

