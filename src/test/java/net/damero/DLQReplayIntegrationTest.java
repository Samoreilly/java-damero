package net.damero;

import com.fasterxml.jackson.databind.ObjectMapper;
import net.damero.Kafka.Annotations.CustomKafkaListener;
import net.damero.Kafka.Config.CustomKafkaAutoConfiguration;
import net.damero.Kafka.CustomObject.EventMetadata;
import net.damero.Kafka.CustomObject.EventWrapper;
import net.damero.Kafka.DeadLetterQueueAPI.DLQController;
import net.damero.Kafka.DeadLetterQueueAPI.ReplayDLQ.ReplayDLQ;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.EnableAspectJAutoProxy;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration test for DLQ replay functionality.
 * Tests the ReplayDLQ class and the /dlq/replay endpoint in DLQController.
 */
@SpringBootTest
@SpringJUnitConfig
@EmbeddedKafka(partitions = 1, topics = {
    "replay-source-topic-1",
    "replay-dlq-topic-1",
    "replay-source-topic-2",
    "replay-dlq-topic-2",
    "replay-source-topic-3",
    "replay-dlq-topic-3",
    "replay-source-topic-4",
    "replay-dlq-topic-4",
    "replay-source-topic-5",
    "replay-dlq-topic-5",
    "replay-source-topic-6",
    "replay-dlq-topic-6"
})
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class DLQReplayIntegrationTest {

    @Autowired
    private EmbeddedKafkaBroker embeddedKafka;

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    @Autowired
    private ReplayDLQ replayDLQ;

    @Autowired
    private DLQController dlqController;

    @Autowired
    private ReplayTestListener replayTestListener;

    @Autowired
    private ObjectMapper kafkaObjectMapper;

    @BeforeEach
    void setUp() throws Exception {
        // Reset listener state
        replayTestListener.reset();

        // Give Kafka listeners time to fully initialize
        Thread.sleep(2000);
    }


    @Test
    void testReplayMessages_SuccessfullyReplaysToDLQ() throws Exception {
        // Given: Create test events with metadata indicating original topic
        TestEvent event1 = new TestEvent("replay-1", "First message", false);
        TestEvent event2 = new TestEvent("replay-2", "Second message", false);
        TestEvent event3 = new TestEvent("replay-3", "Third message", false);

        EventMetadata metadata1 = createMetadata("replay-source-topic-1", "replay-dlq-topic-1");
        EventMetadata metadata2 = createMetadata("replay-source-topic-1", "replay-dlq-topic-1");
        EventMetadata metadata3 = createMetadata("replay-source-topic-1", "replay-dlq-topic-1");

        EventWrapper<TestEvent> wrapper1 = new EventWrapper<>(event1, LocalDateTime.now(), metadata1);
        EventWrapper<TestEvent> wrapper2 = new EventWrapper<>(event2, LocalDateTime.now(), metadata2);
        EventWrapper<TestEvent> wrapper3 = new EventWrapper<>(event3, LocalDateTime.now(), metadata3);

        // Set up expectation for replayed messages BEFORE sending to DLQ
        replayTestListener.setTopicToListenFor("replay-source-topic-1");
        replayTestListener.expectMessages(3);

        // When: Send events to DLQ topic
        kafkaTemplate.send("replay-dlq-topic-1", wrapper1).get(5, TimeUnit.SECONDS);
        kafkaTemplate.send("replay-dlq-topic-1", wrapper2).get(5, TimeUnit.SECONDS);
        kafkaTemplate.send("replay-dlq-topic-1", wrapper3).get(5, TimeUnit.SECONDS);

        // Wait for messages to be fully committed to DLQ
        Thread.sleep(5000);

        System.out.println("DEBUG: About to replay messages from replay-dlq-topic-1");
        System.out.println("DEBUG: Current listener state - Topic filter: " + replayTestListener.topicToListenFor +
            ", Expected count: " + replayTestListener.latch.getCount());

        // Then: Replay messages from DLQ (force from beginning for testing)
        ReplayDLQ.ReplayResult result = replayDLQ.replayMessages("replay-dlq-topic-1", true);

        System.out.println("DEBUG: Replay completed - Success: " + result.getSuccessCount() +
            ", Failures: " + result.getFailureCount());

        // Verify replay result
        assertNotNull(result, "Replay result should not be null");
        assertEquals(3, result.getSuccessCount(), "Should successfully replay 3 messages");
        assertEquals(0, result.getFailureCount(), "Should have no failures");
        assertEquals(3, result.getTotalProcessed(), "Should process 3 messages total");

        // Wait longer for messages to be produced and consumed
        Thread.sleep(5000);

        // Verify messages were received on original topic - give even more time for async processing
        assertTrue(replayTestListener.awaitExpectedMessages(30, TimeUnit.SECONDS),
            "Should receive all 3 replayed messages");

        List<TestEvent> receivedEvents = replayTestListener.getReceivedEvents();
        assertEquals(3, receivedEvents.size(), "Should receive 3 events");
        
        // Verify event content
        assertTrue(receivedEvents.stream().anyMatch(e -> "replay-1".equals(e.getId())),
            "Should contain first event");
        assertTrue(receivedEvents.stream().anyMatch(e -> "replay-2".equals(e.getId())),
            "Should contain second event");
        assertTrue(receivedEvents.stream().anyMatch(e -> "replay-3".equals(e.getId())),
            "Should contain third event");
    }

    @Test
    void testReplayMessages_HandlesEmptyDLQ() {
        // When: Replay from empty DLQ
        ReplayDLQ.ReplayResult result = replayDLQ.replayMessages("replay-dlq-topic-2", true);

        // Then: Should complete successfully with no messages
        assertNotNull(result, "Replay result should not be null");
        assertEquals(0, result.getSuccessCount(), "Should have no successes");
        assertEquals(0, result.getFailureCount(), "Should have no failures");
        assertEquals(0, result.getTotalProcessed(), "Should process 0 messages");
    }

    @Test
    void testReplayMessages_ValidatesInput() {
        // When/Then: Should throw exception for null topic
        assertThrows(IllegalArgumentException.class, () ->
            replayDLQ.replayMessages(null)
        , "Should throw exception for null topic");

        // When/Then: Should throw exception for empty topic
        assertThrows(IllegalArgumentException.class, () ->
            replayDLQ.replayMessages("")
        , "Should throw exception for empty topic");

        // When/Then: Should throw exception for blank topic
        assertThrows(IllegalArgumentException.class, () ->
            replayDLQ.replayMessages("   ")
        , "Should throw exception for blank topic");
    }

    @Test
    void testReplayMessages_HandlesMalformedMessages() throws Exception {
        // Given: Send malformed JSON to DLQ
        Map<String, Object> producerProps = KafkaTestUtils.producerProps(embeddedKafka);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        
        try (org.apache.kafka.clients.producer.KafkaProducer<String, String> producer = 
                new org.apache.kafka.clients.producer.KafkaProducer<>(producerProps)) {
            
            // Send valid message
            TestEvent validEvent = new TestEvent("valid", "Valid message", false);
            EventMetadata validMetadata = createMetadata("replay-source-topic-3", "replay-dlq-topic-3");
            EventWrapper<TestEvent> validWrapper = new EventWrapper<>(validEvent, LocalDateTime.now(), validMetadata);
            String validJson = kafkaObjectMapper.writeValueAsString(validWrapper);
            producer.send(new org.apache.kafka.clients.producer.ProducerRecord<>("replay-dlq-topic-3", validJson)).get();

            // Send malformed message
            producer.send(new org.apache.kafka.clients.producer.ProducerRecord<>("replay-dlq-topic-3", "{invalid-json}")).get();

            // Send another valid message
            TestEvent validEvent2 = new TestEvent("valid2", "Another valid message", false);
            EventMetadata validMetadata2 = createMetadata("replay-source-topic-3", "replay-dlq-topic-3");
            EventWrapper<TestEvent> validWrapper2 = new EventWrapper<>(validEvent2, LocalDateTime.now(), validMetadata2);
            String validJson2 = kafkaObjectMapper.writeValueAsString(validWrapper2);
            producer.send(new org.apache.kafka.clients.producer.ProducerRecord<>("replay-dlq-topic-3", validJson2)).get();
        }

        // Wait for messages to be fully committed
        Thread.sleep(2000);

        // When: Replay messages (force from beginning for testing)
        ReplayDLQ.ReplayResult result = replayDLQ.replayMessages("replay-dlq-topic-3", true);

        // Then: Should replay valid messages and track errors
        assertNotNull(result, "Replay result should not be null");
        assertEquals(2, result.getSuccessCount(), "Should successfully replay 2 valid messages");
        assertEquals(1, result.getFailureCount(), "Should have 1 failure for malformed message");
        assertEquals(3, result.getTotalProcessed(), "Should process 3 messages total");
        
        assertTrue(result.getErrorsPerType().containsKey("deserialization_errors"),
            "Should track deserialization errors");
        assertEquals(1, result.getErrorsPerType().get("deserialization_errors"),
            "Should have 1 deserialization error");
    }

    @Test
    void testReplayMessages_HandlesMissingOriginalTopic() throws Exception {
        // Given: Create event wrapper WITHOUT original topic metadata
        TestEvent event = new TestEvent("no-topic", "Message without original topic", false);
        EventMetadata metadataWithoutTopic = EventMetadata.builder()
            .attempts(3)
            .dlqTopic("replay-dlq-topic-4")
            .originalTopic(null) // No original topic
            .build();
        
        EventWrapper<TestEvent> wrapper = new EventWrapper<>(event, LocalDateTime.now(), metadataWithoutTopic);

        // When: Send to DLQ and replay
        kafkaTemplate.send("replay-dlq-topic-4", wrapper).get(5, TimeUnit.SECONDS);

        // Wait for message to be fully committed
        Thread.sleep(2000);

        ReplayDLQ.ReplayResult result = replayDLQ.replayMessages("replay-dlq-topic-4", true);

        // Then: Should fail to replay message without original topic
        assertNotNull(result, "Replay result should not be null");
        assertEquals(0, result.getSuccessCount(), "Should not successfully replay messages without original topic");
        assertEquals(1, result.getFailureCount(), "Should have 1 failure");
    }

    @Test
    void testDLQController_ReplayEndpoint() throws Exception {
        // Given: Messages in DLQ
        TestEvent event1 = new TestEvent("controller-1", "Controller test message 1", false);
        TestEvent event2 = new TestEvent("controller-2", "Controller test message 2", false);

        EventMetadata metadata1 = createMetadata("replay-source-topic-5", "replay-dlq-topic-5");
        EventMetadata metadata2 = createMetadata("replay-source-topic-5", "replay-dlq-topic-5");

        EventWrapper<TestEvent> wrapper1 = new EventWrapper<>(event1, LocalDateTime.now(), metadata1);
        EventWrapper<TestEvent> wrapper2 = new EventWrapper<>(event2, LocalDateTime.now(), metadata2);

        // Set up expectation for replayed messages BEFORE sending to DLQ
        replayTestListener.setTopicToListenFor("replay-source-topic-5");
        replayTestListener.expectMessages(2);

        kafkaTemplate.send("replay-dlq-topic-5", wrapper1).get(5, TimeUnit.SECONDS);
        kafkaTemplate.send("replay-dlq-topic-5", wrapper2).get(5, TimeUnit.SECONDS);

        // Wait for messages to be fully committed to DLQ
        Thread.sleep(2000);

        System.out.println("DEBUG: About to call controller replay endpoint for replay-dlq-topic-5");

        // When: Call controller endpoint with forceReplay=true and skipValidation=false for testing
        dlqController.replayDLQEndpoint("replay-dlq-topic-5", true, false);

        // Wait a bit for replay to complete
        Thread.sleep(1000);

        System.out.println("DEBUG: Controller replay endpoint called, waiting for messages");

        // Then: Verify messages were replayed - give more time for async processing
        boolean received = replayTestListener.awaitExpectedMessages(20, TimeUnit.SECONDS);

        List<TestEvent> receivedEvents = replayTestListener.getReceivedEvents();
        System.out.println("DEBUG: Received " + receivedEvents.size() + " events from topic replay-source-topic-5");
        for (TestEvent event : receivedEvents) {
            System.out.println("DEBUG: Event ID: " + event.getId());
        }

        assertTrue(received,
            "Should receive replayed messages via controller endpoint");

        assertEquals(2, receivedEvents.size(), "Should receive 2 events");
    }

    private EventMetadata createMetadata(String originalTopic, String dlqTopic) {
        return EventMetadata.builder()
            .attempts(3)
            .firstFailureDateTime(LocalDateTime.now().minusMinutes(5))
            .lastFailureDateTime(LocalDateTime.now())
            .originalTopic(originalTopic)
            .dlqTopic(dlqTopic)
            .build();
    }



    // ==================== Test Configuration ====================

    @Configuration
    @EnableKafka
    @EnableAspectJAutoProxy
    @Import(CustomKafkaAutoConfiguration.class)
    @ComponentScan(basePackages = "net.damero")
    static class TestConfig {

        @Autowired
        private EmbeddedKafkaBroker embeddedKafka;

        @Bean
        public ProducerFactory<String, Object> producerFactory(ObjectMapper kafkaObjectMapper) {
            Map<String, Object> props = new HashMap<>(KafkaTestUtils.producerProps(embeddedKafka));
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

            DefaultKafkaProducerFactory<String, Object> factory = new DefaultKafkaProducerFactory<>(props);
            JsonSerializer<Object> serializer = new JsonSerializer<>(kafkaObjectMapper);
            serializer.setAddTypeInfo(true);
            factory.setValueSerializer(serializer);

            return factory;
        }

        @Bean
        public KafkaTemplate<String, Object> kafkaTemplate(ProducerFactory<String, Object> producerFactory) {
            return new KafkaTemplate<>(producerFactory);
        }

        @Bean
        public ConsumerFactory<String, Object> consumerFactory(ObjectMapper kafkaObjectMapper) {
            Map<String, Object> props = new HashMap<>(KafkaTestUtils.consumerProps("replay-test-group", "false", embeddedKafka));
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);  // Disable auto-commit for MANUAL ack mode

            JsonDeserializer<Object> deserializer = new JsonDeserializer<>(kafkaObjectMapper);
            deserializer.addTrustedPackages("*");
            deserializer.setUseTypeHeaders(true);

            return new DefaultKafkaConsumerFactory<>(props, new StringDeserializer(), deserializer);
        }

        @Bean
        public ConcurrentKafkaListenerContainerFactory<String, Object> kafkaListenerContainerFactory(
                ConsumerFactory<String, Object> consumerFactory) {
            ConcurrentKafkaListenerContainerFactory<String, Object> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
            factory.setConsumerFactory(consumerFactory);
            factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.RECORD);
            return factory;
        }

        @Bean
        public ReplayTestListener replayTestListener() {
            return new ReplayTestListener();
        }
    }

    // ==================== Test Listener ====================

    static class ReplayTestListener {
        private final CopyOnWriteArrayList<TestEvent> receivedEvents = new CopyOnWriteArrayList<>();
        private volatile CountDownLatch latch = new CountDownLatch(0);
        private volatile String topicToListenFor = null;

        @org.springframework.kafka.annotation.KafkaListener(topics = "replay-source-topic-1", groupId = "replay-test-listener-1-#{T(java.util.UUID).randomUUID().toString()}")
        public void listenTopic1(TestEvent event) {
            handleEvent(event, "replay-source-topic-1");
        }

        @org.springframework.kafka.annotation.KafkaListener(topics = "replay-source-topic-2", groupId = "replay-test-listener-2-#{T(java.util.UUID).randomUUID().toString()}")
        public void listenTopic2(TestEvent event) {
            handleEvent(event, "replay-source-topic-2");
        }

        @org.springframework.kafka.annotation.KafkaListener(topics = "replay-source-topic-3", groupId = "replay-test-listener-3-#{T(java.util.UUID).randomUUID().toString()}")
        public void listenTopic3(TestEvent event) {
            handleEvent(event, "replay-source-topic-3");
        }

        @org.springframework.kafka.annotation.KafkaListener(topics = "replay-source-topic-4", groupId = "replay-test-listener-4-#{T(java.util.UUID).randomUUID().toString()}")
        public void listenTopic4(TestEvent event) {
            handleEvent(event, "replay-source-topic-4");
        }

        @org.springframework.kafka.annotation.KafkaListener(topics = "replay-source-topic-5", groupId = "replay-test-listener-5-#{T(java.util.UUID).randomUUID().toString()}")
        public void listenTopic5(TestEvent event) {
            handleEvent(event, "replay-source-topic-5");
        }

        @org.springframework.kafka.annotation.KafkaListener(topics = "replay-source-topic-6", groupId = "replay-test-listener-6-#{T(java.util.UUID).randomUUID().toString()}")
        public void listenTopic6(TestEvent event) {
            handleEvent(event, "replay-source-topic-6");
        }

        private void handleEvent(TestEvent event, String receivedFromTopic) {
            System.out.println("DEBUG: handleEvent called - Topic: " + receivedFromTopic +
                ", Event ID: " + event.getId() +
                ", Listening for: " + topicToListenFor +
                ", Latch count: " + latch.getCount());

            // Only count events from the topic we're currently testing
            if (topicToListenFor == null || topicToListenFor.equals(receivedFromTopic)) {
                receivedEvents.add(event);
                latch.countDown();
                System.out.println("DEBUG: Event added - Total received: " + receivedEvents.size() +
                    ", Remaining latch: " + latch.getCount());
            } else {
                System.out.println("DEBUG: Event ignored - not listening for topic " + receivedFromTopic);
            }
        }

        public void setTopicToListenFor(String topic) {
            this.topicToListenFor = topic;
        }

        public void expectMessages(int count) {
            latch = new CountDownLatch(count);
        }

        public boolean awaitMessages(int count, long timeout, TimeUnit unit) throws InterruptedException {
            expectMessages(count);
            return latch.await(timeout, unit);
        }

        public boolean awaitExpectedMessages(long timeout, TimeUnit unit) throws InterruptedException {
            return latch.await(timeout, unit);
        }

        public List<TestEvent> getReceivedEvents() {
            return List.copyOf(receivedEvents);
        }

        public void reset() {
            receivedEvents.clear();
            latch = new CountDownLatch(0);
            topicToListenFor = null;
        }
    }

    // ==================== Test Event ====================

    static class TestEvent {
        private String id;
        private String message;
        private boolean shouldFail;

        public TestEvent() {
        }

        public TestEvent(String id, String message, boolean shouldFail) {
            this.id = id;
            this.message = message;
            this.shouldFail = shouldFail;
        }

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public String getMessage() {
            return message;
        }

        public void setMessage(String message) {
            this.message = message;
        }

        public boolean isShouldFail() {
            return shouldFail;
        }

        public void setShouldFail(boolean shouldFail) {
            this.shouldFail = shouldFail;
        }

        @Override
        public String toString() {
            return "TestEvent{id='" + id + "', message='" + message + "', shouldFail=" + shouldFail + "}";
        }
    }
}

