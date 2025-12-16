package net.damero;

import com.fasterxml.jackson.databind.ObjectMapper;
import net.damero.Kafka.Annotations.DameroKafkaListener;
import net.damero.Kafka.Config.CustomKafkaAutoConfiguration;
import net.damero.Kafka.Config.DelayMethod;
import net.damero.Kafka.CustomObject.EventWrapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.*;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

// mvn test -DexcludedTests=StandaloneMultiPartitionTest
// THIS FILE MUST BE RUN ALONE AS IT INTERFERES WITH THE EMBEDDED KAFKA CONTEXT OF OTHER TESTS
/**
 * multi partition integration test.
 * tests that the library correctly handles messages across multiple partitions.
 * verifies retry counts, concurrent processing, and dlq routing work correctly
 * when messages are distributed across partitions.
 */
@SpringBootTest(classes = StandaloneMultiPartitionTest.TestConfig.class)
@EmbeddedKafka(
        partitions = 3,
        topics = {"multi-partition-topic", "multi-partition-retry", "multi-partition-dlq"},
        brokerProperties = {
                "listeners=PLAINTEXT://localhost:0"
        }
)
@AutoConfigureMockMvc
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class StandaloneMultiPartitionTest {

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    @Autowired
    private MultiPartitionTestListener testListener;

    @Autowired
    private MultiPartitionDLQCollector dlqCollector;

    @BeforeEach
    void setUp() throws Exception {
        testListener.reset();
        dlqCollector.reset();
        // wait for kafka to be ready
        Thread.sleep(1000);
    }

    @Test
    void testMessagesProcessedFromAllPartitions() throws Exception {
        // given: messages with keys that will distribute across partitions
        // kafka uses key hash to determine partition, so different keys go to different partitions
        TestEvent event0 = new TestEvent("partition-key-0", "data-0", false);
        TestEvent event1 = new TestEvent("partition-key-1", "data-1", false);
        TestEvent event2 = new TestEvent("partition-key-2", "data-2", false);

        // when: send messages with different keys to distribute across partitions
        kafkaTemplate.send("multi-partition-topic", "key-0", event0).get(5, TimeUnit.SECONDS);
        kafkaTemplate.send("multi-partition-topic", "key-1", event1).get(5, TimeUnit.SECONDS);
        kafkaTemplate.send("multi-partition-topic", "key-2", event2).get(5, TimeUnit.SECONDS);

        // then: all messages should be processed successfully
        await().atMost(20, TimeUnit.SECONDS).untilAsserted(() -> {
            List<TestEvent> successful = testListener.getSuccessfulEvents();
            assertEquals(3, successful.size(), "should process all 3 messages");

            Set<String> processedIds = successful.stream()
                    .map(TestEvent::getId)
                    .collect(Collectors.toSet());

            assertTrue(processedIds.contains("partition-key-0"), "should process event from key-0");
            assertTrue(processedIds.contains("partition-key-1"), "should process event from key-1");
            assertTrue(processedIds.contains("partition-key-2"), "should process event from key-2");
        });
    }

    @Test
    void testRetryCountsAccurateAcrossPartitions() throws Exception {
        // given: messages that will fail and need retries, distributed across partitions
        TestEvent failEvent0 = new TestEvent("fail-partition-0", "data", true);
        TestEvent failEvent1 = new TestEvent("fail-partition-1", "data", true);
        TestEvent failEvent2 = new TestEvent("fail-partition-2", "data", true);

        // when: send failing messages with different keys
        kafkaTemplate.send("multi-partition-topic", "fail-key-0", failEvent0).get(5, TimeUnit.SECONDS);
        kafkaTemplate.send("multi-partition-topic", "fail-key-1", failEvent1).get(5, TimeUnit.SECONDS);
        kafkaTemplate.send("multi-partition-topic", "fail-key-2", failEvent2).get(5, TimeUnit.SECONDS);

        // then: each message should retry 3 times and go to dlq
        await().atMost(60, TimeUnit.SECONDS).untilAsserted(() -> {
            // verify each message was attempted correct number of times
            int attempts0 = testListener.getAttemptCount("fail-partition-0");
            int attempts1 = testListener.getAttemptCount("fail-partition-1");
            int attempts2 = testListener.getAttemptCount("fail-partition-2");

            assertEquals(3, attempts0, "fail-partition-0 should have 3 attempts");
            assertEquals(3, attempts1, "fail-partition-1 should have 3 attempts");
            assertEquals(3, attempts2, "fail-partition-2 should have 3 attempts");

            // verify all went to dlq
            List<EventWrapper<?>> dlqMessages = dlqCollector.getAllMessages();
            assertEquals(3, dlqMessages.size(), "should have 3 messages in dlq");
        });
    }

    @Test
    void testMixedSuccessAndFailureAcrossPartitions() throws Exception {
        // given: mix of successful and failing messages
        TestEvent success0 = new TestEvent("success-mp-0", "data", false);
        TestEvent success1 = new TestEvent("success-mp-1", "data", false);
        TestEvent fail0 = new TestEvent("fail-mp-0", "data", true);
        TestEvent fail1 = new TestEvent("fail-mp-1", "data", true);

        // when: send mixed messages
        kafkaTemplate.send("multi-partition-topic", "skey-0", success0).get(5, TimeUnit.SECONDS);
        kafkaTemplate.send("multi-partition-topic", "fkey-0", fail0).get(5, TimeUnit.SECONDS);
        kafkaTemplate.send("multi-partition-topic", "skey-1", success1).get(5, TimeUnit.SECONDS);
        kafkaTemplate.send("multi-partition-topic", "fkey-1", fail1).get(5, TimeUnit.SECONDS);

        // then: successful ones processed, failed ones go to dlq
        await().atMost(60, TimeUnit.SECONDS).untilAsserted(() -> {
            // successful messages should be processed
            List<TestEvent> successful = testListener.getSuccessfulEvents();
            assertEquals(2, successful.size(), "should have 2 successful messages");

            Set<String> successIds = successful.stream()
                    .map(TestEvent::getId)
                    .collect(Collectors.toSet());
            assertTrue(successIds.contains("success-mp-0"));
            assertTrue(successIds.contains("success-mp-1"));

            // failed messages should be in dlq
            List<EventWrapper<?>> dlqMessages = dlqCollector.getAllMessages();
            assertEquals(2, dlqMessages.size(), "should have 2 messages in dlq");

            Set<String> dlqIds = dlqMessages.stream()
                    .filter(wrapper -> wrapper.getEvent() instanceof TestEvent)
                    .map(wrapper -> ((TestEvent) wrapper.getEvent()).getId())
                    .collect(Collectors.toSet());
            assertTrue(dlqIds.contains("fail-mp-0"));
            assertTrue(dlqIds.contains("fail-mp-1"));
        });
    }

    @Test
    void testConcurrentProcessingFromMultiplePartitions() throws Exception {
        // given: many messages that will be processed concurrently
        int messageCount = 10;
        CountDownLatch processingLatch = new CountDownLatch(messageCount);
        testListener.setProcessingLatch(processingLatch);

        // when: send multiple messages rapidly
        for (int i = 0; i < messageCount; i++) {
            TestEvent event = new TestEvent("concurrent-" + i, "data-" + i, false);
            kafkaTemplate.send("multi-partition-topic", "concurrent-key-" + i, event);
        }

        // then: all messages should be processed
        boolean allProcessed = processingLatch.await(30, TimeUnit.SECONDS);
        assertTrue(allProcessed, "all messages should be processed within timeout");

        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            List<TestEvent> successful = testListener.getSuccessfulEvents();
            assertEquals(messageCount, successful.size(), "should process all " + messageCount + " messages");
        });
    }

    @Test
    void testPartitionAwarenessInMetadata() throws Exception {
        // given: a failing message
        TestEvent failEvent = new TestEvent("metadata-partition-test", "data", true);

        // when: send message
        kafkaTemplate.send("multi-partition-topic", "metadata-key", failEvent).get(5, TimeUnit.SECONDS);

        // then: dlq message should have correct metadata
        await().atMost(60, TimeUnit.SECONDS).untilAsserted(() -> {
            List<EventWrapper<?>> dlqMessages = dlqCollector.getAllMessages();
            assertFalse(dlqMessages.isEmpty(), "should have dlq message");

            EventWrapper<?> dlqMessage = dlqMessages.stream()
                    .filter(wrapper -> wrapper.getEvent() instanceof TestEvent)
                    .filter(wrapper -> "metadata-partition-test".equals(((TestEvent) wrapper.getEvent()).getId()))
                    .findFirst()
                    .orElse(null);

            assertNotNull(dlqMessage, "should find our test message in dlq");
            assertNotNull(dlqMessage.getMetadata(), "metadata should not be null");
            assertEquals(3, dlqMessage.getMetadata().getAttempts(), "should have 3 attempts");
            assertEquals("multi-partition-topic", dlqMessage.getMetadata().getOriginalTopic());
            assertEquals("multi-partition-dlq", dlqMessage.getMetadata().getDlqTopic());
        });
    }

    @Test
    void testRetryIsolationAcrossPartitions() throws Exception {
        // given: messages where one fails temporarily and one always fails
        // this tests that retry isolation works across partitions
        TestEvent alwaysFails = new TestEvent("always-fails-mp", "data", true);
        TestEvent succeedsEventually = new TestEvent("succeeds-eventually-mp", "data", true);

        // set up the listener to succeed on 2nd attempt for one message
        testListener.setSucceedOnAttempt("succeeds-eventually-mp", 2);

        // when: send both messages
        kafkaTemplate.send("multi-partition-topic", "always-fails-key", alwaysFails).get(5, TimeUnit.SECONDS);
        kafkaTemplate.send("multi-partition-topic", "succeeds-key", succeedsEventually).get(5, TimeUnit.SECONDS);

        // then: one should succeed, one should go to dlq
        await().atMost(60, TimeUnit.SECONDS).untilAsserted(() -> {
            // succeeds-eventually should be in successful events
            List<TestEvent> successful = testListener.getSuccessfulEvents();
            boolean hasSucceedsEventually = successful.stream()
                    .anyMatch(e -> "succeeds-eventually-mp".equals(e.getId()));
            assertTrue(hasSucceedsEventually, "succeeds-eventually-mp should eventually succeed");

            // always-fails should be in dlq
            List<EventWrapper<?>> dlqMessages = dlqCollector.getAllMessages();
            boolean hasAlwaysFails = dlqMessages.stream()
                    .filter(wrapper -> wrapper.getEvent() instanceof TestEvent)
                    .anyMatch(wrapper -> "always-fails-mp".equals(((TestEvent) wrapper.getEvent()).getId()));
            assertTrue(hasAlwaysFails, "always-fails-mp should be in dlq");
        });
    }


    // ==================== test configuration ====================

    @Configuration
    @EnableKafka
    @EnableAspectJAutoProxy
    @Import(CustomKafkaAutoConfiguration.class)
    @ComponentScan(basePackages = "net.damero")
    static class TestConfig {

        @Autowired
        private EmbeddedKafkaBroker embeddedKafka;

        @Bean
        public ObjectMapper kafkaObjectMapper() {
            ObjectMapper mapper = new ObjectMapper();
            mapper.findAndRegisterModules();
            return mapper;
        }

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
            Map<String, Object> props = new HashMap<>(KafkaTestUtils.consumerProps("multi-partition-group", "false", embeddedKafka));
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

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
            // enable concurrency matching partition count
            factory.setConcurrency(3);
            return factory;
        }

        @Bean
        public MultiPartitionTestListener multiPartitionTestListener() {
            return new MultiPartitionTestListener();
        }

        @Bean
        public MultiPartitionDLQCollector multiPartitionDLQCollector() {
            return new MultiPartitionDLQCollector();
        }
    }


    // ==================== test listener ====================

    static class MultiPartitionTestListener {
        private final CopyOnWriteArrayList<TestEvent> successfulEvents = new CopyOnWriteArrayList<>();
        private final CopyOnWriteArrayList<TestEvent> failedEvents = new CopyOnWriteArrayList<>();
        private final ConcurrentHashMap<String, AtomicInteger> attemptCounts = new ConcurrentHashMap<>();
        private final ConcurrentHashMap<String, Integer> succeedOnAttemptMap = new ConcurrentHashMap<>();
        private volatile CountDownLatch processingLatch = null;

        @DameroKafkaListener(
                topic = "multi-partition-topic",
                retryableTopic = "multi-partition-retry",
                dlqTopic = "multi-partition-dlq",
                maxAttempts = 3,
                delay = 100,
                delayMethod = DelayMethod.LINEAR
        )
        @KafkaListener(
                topics = "multi-partition-topic",
                groupId = "multi-partition-test-group",
                containerFactory = "kafkaListenerContainerFactory"
        )
        public void listen(ConsumerRecord<String, Object> record) {
            if (record == null) {
                return;
            }

            Object payload = record.value();
            TestEvent event = extractEvent(payload);

            if (event == null) {
                return;
            }

            String eventId = event.getId();
            int currentAttempt = attemptCounts.computeIfAbsent(eventId, k -> new AtomicInteger(0))
                    .incrementAndGet();

            // check if this event should succeed on a specific attempt
            Integer succeedOnAttempt = succeedOnAttemptMap.get(eventId);
            boolean shouldSucceed = !event.isShouldFail() ||
                    (succeedOnAttempt != null && currentAttempt >= succeedOnAttempt);

            if (!shouldSucceed) {
                failedEvents.add(event);
                throw new RuntimeException("simulated failure for event: " + eventId + " (attempt " + currentAttempt + ")");
            }

            successfulEvents.add(event);
            if (processingLatch != null) {
                processingLatch.countDown();
            }
        }

        private TestEvent extractEvent(Object payload) {
            if (payload instanceof TestEvent te) {
                return te;
            } else if (payload instanceof Map<?, ?> map) {
                Object id = map.get("id");
                Object data = map.get("data");
                Object shouldFail = map.get("shouldFail");
                if (id instanceof String) {
                    return new TestEvent((String) id, data != null ? data.toString() : null, Boolean.TRUE.equals(shouldFail));
                }
            }
            return null;
        }

        public List<TestEvent> getSuccessfulEvents() {
            return List.copyOf(successfulEvents);
        }

        public List<TestEvent> getFailedEvents() {
            return List.copyOf(failedEvents);
        }

        public int getAttemptCount(String eventId) {
            AtomicInteger count = attemptCounts.get(eventId);
            return count != null ? count.get() : 0;
        }

        public void setSucceedOnAttempt(String eventId, int attemptNumber) {
            succeedOnAttemptMap.put(eventId, attemptNumber);
        }

        public void setProcessingLatch(CountDownLatch latch) {
            this.processingLatch = latch;
        }

        public void reset() {
            successfulEvents.clear();
            failedEvents.clear();
            attemptCounts.clear();
            succeedOnAttemptMap.clear();
            processingLatch = null;
        }
    }


    // ==================== dlq collector ====================

    static class MultiPartitionDLQCollector {
        private final CopyOnWriteArrayList<EventWrapper<?>> dlqMessages = new CopyOnWriteArrayList<>();

        @KafkaListener(
                topics = "multi-partition-dlq",
                groupId = "multi-partition-dlq-collector",
                containerFactory = "kafkaListenerContainerFactory"
        )
        public void collect(ConsumerRecord<String, Object> record) {
            if (record == null || record.value() == null) {
                return;
            }

            Object payload = record.value();
            if (payload instanceof EventWrapper<?> wrapper) {
                dlqMessages.add(wrapper);
            }
        }

        public List<EventWrapper<?>> getAllMessages() {
            return List.copyOf(dlqMessages);
        }

        public EventWrapper<?> getLastMessage() {
            return dlqMessages.isEmpty() ? null : dlqMessages.get(dlqMessages.size() - 1);
        }

        public void reset() {
            dlqMessages.clear();
        }
    }
}

