package net.damero;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.BasicPolymorphicTypeValidator;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import net.damero.Annotations.CustomKafkaListener;
import net.damero.CustomKafkaSetup.CustomKafkaAutoConfiguration;
import net.damero.CustomKafkaSetup.RetryKafkaListener.KafkaRetryListener;
import net.damero.CustomKafkaSetup.RetryKafkaListener.RetryDelayCalculator;
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
import org.springframework.context.annotation.Primary;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.stereotype.Component;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(SpringExtension.class)
@SpringBootTest(
        classes = {
                RetryMechanismIntegrationTest.TestConfig.class,
                RetryMechanismIntegrationTest.RetryTestListener.class,
                RetryMechanismIntegrationTest.RetryDLQCollector.class,
                CustomKafkaAutoConfiguration.class
        }

)
@EmbeddedKafka(
        partitions = 1,
        topics = {"retry-test-topic", "retry-test-retry", "retry-test-dlq"}
)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class RetryMechanismIntegrationTest {

    @Autowired
    private KafkaTemplate<String, TestEvent> kafkaTemplate;

    @Autowired
    private RetryTestListener retryTestListener;

    @Autowired
    private RetryDLQCollector dlqCollector;

    // ❌ REMOVED: Wrong autowired bean from different test
    // @Autowired
    // private NonBlockingRetryDelayTest.DelayTestListener delayTestListener;

    @BeforeEach
    void setUp() {
        retryTestListener.reset();
        dlqCollector.reset();
        // Give Kafka listeners time to be ready
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Test
    void testRetryListenerIsCreatedDynamically() {
        TestEvent event = new TestEvent(UUID.randomUUID().toString(), "Retry flow", true);
        kafkaTemplate.send("retry-test-topic", event);

        // Should be processed multiple times through retry mechanism
        await().atMost(20, TimeUnit.SECONDS)
                .pollInterval(500, TimeUnit.MILLISECONDS)
                .untilAsserted(() -> {
                    int processedCount = retryTestListener.getProcessedCount();
                    assertTrue(processedCount >= 2,
                            "Event should be processed at least twice (original + 1 retry), got: " + processedCount);
                });
    }

    @Test
    void testRetryPreservesEventData() {
        TestEvent event = new TestEvent(UUID.randomUUID().toString(), "Data preservation", true);
        kafkaTemplate.send("retry-test-topic", event);

        await().atMost(20, TimeUnit.SECONDS)
                .pollInterval(500, TimeUnit.MILLISECONDS)
                .untilAsserted(() -> {
                    assertEquals(1, dlqCollector.getReceivedMessages().size(),
                            "Should have exactly 1 message in DLQ");

                    EventWrapper<?> wrapper = dlqCollector.getReceivedMessages().get(0);
                    assertNotNull(wrapper, "DLQ wrapper should not be null");
                    assertNotNull(wrapper.getEvent(), "DLQ event should not be null");

                    TestEvent dlqEvent = (TestEvent) wrapper.getEvent();
                    assertEquals(event.getId(), dlqEvent.getId());
                    assertEquals(event.getMessage(), dlqEvent.getMessage());
                    assertEquals(event.isShouldFail(), dlqEvent.isShouldFail());
                });
    }

    @Test
    void testRetryMetadataIncrements() {
        TestEvent event = new TestEvent(UUID.randomUUID().toString(), "Metadata increment", true);
        kafkaTemplate.send("retry-test-topic", event);

        await().atMost(20, TimeUnit.SECONDS)
                .pollInterval(500, TimeUnit.MILLISECONDS)
                .untilAsserted(() -> {
                    assertEquals(1, dlqCollector.getReceivedMessages().size(),
                            "Should have message in DLQ");

                    EventWrapper<?> wrapper = dlqCollector.getReceivedMessages().get(0);
                    assertNotNull(wrapper.getMetadata(), "Metadata should not be null");

                    assertEquals(3, wrapper.getMetadata().getAttempts(),
                            "Metadata should show 3 attempts");
                    assertEquals("retry-test-topic", wrapper.getMetadata().getOriginalTopic(),
                            "Should preserve original topic");
                });
    }

    @Test
    void testSuccessfulEventBypassesRetry() {
        TestEvent event = new TestEvent(UUID.randomUUID().toString(), "Success", false);
        kafkaTemplate.send("retry-test-topic", event);

        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            assertTrue(retryTestListener.getProcessedCount() >= 1,
                    "Should process successful event at least once");
        });

        // Wait a bit more to ensure no retries happen
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        assertEquals(0, dlqCollector.getReceivedMessages().size(),
                "Successful events should not go to DLQ");

        // Should only be processed once (no retries needed)
        assertEquals(1, retryTestListener.getProcessedCount(),
                "Successful event should only be processed once");
    }

    @Test
    void testEventuallySuccessfulAfterRetry() {
        TestEvent event = new TestEvent(
                UUID.randomUUID().toString(),
                "Eventually succeed",
                true
        );

        // Configure listener to succeed on 2nd attempt
        retryTestListener.setFailUntilAttempt(event.getId(), 2);

        kafkaTemplate.send("retry-test-topic", event);

        await().atMost(20, TimeUnit.SECONDS)
                .pollInterval(500, TimeUnit.MILLISECONDS)
                .untilAsserted(() -> {
                    assertTrue(retryTestListener.getProcessedCount() >= 2,
                            "Should be processed at least twice, got: " + retryTestListener.getProcessedCount());
                    assertTrue(retryTestListener.hasSucceeded(event.getId()),
                            "Should eventually succeed");
                });

        // Wait a bit more to ensure it doesn't go to DLQ after success
        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        assertEquals(0, dlqCollector.getReceivedMessages().size(),
                "Should not go to DLQ if eventually successful");
    }

    @Test
    void testConcurrentRetries() {
        TestEvent event1 = new TestEvent(UUID.randomUUID().toString(), "Concurrent 1", true);
        TestEvent event2 = new TestEvent(UUID.randomUUID().toString(), "Concurrent 2", true);
        TestEvent event3 = new TestEvent(UUID.randomUUID().toString(), "Concurrent 3", true);

        kafkaTemplate.send("retry-test-topic", event1);
        kafkaTemplate.send("retry-test-topic", event2);
        kafkaTemplate.send("retry-test-topic", event3);

        await().atMost(30, TimeUnit.SECONDS)
                .pollInterval(1, TimeUnit.SECONDS)
                .untilAsserted(() -> {
                    int dlqSize = dlqCollector.getReceivedMessages().size();
                    System.out.println("Current DLQ size: " + dlqSize + "/3");
                    assertEquals(3, dlqSize, "All 3 events should reach DLQ");
                });
    }

    @Component
    public static class RetryTestListener {
        private final AtomicInteger processedCount = new AtomicInteger(0);
        private final List<String> successfulEvents = new CopyOnWriteArrayList<>();
        private final Map<String, Integer> failUntilAttempt = new ConcurrentHashMap<>();
        private final Map<String, AtomicInteger> eventAttempts = new ConcurrentHashMap<>();

        @CustomKafkaListener(
                topic = "retry-test-topic",
                retryable = true,
                retryableTopic = "retry-test-retry",
                dlqTopic = "retry-test-dlq",
                maxAttempts = 3,
                delay = 100
        )
        @KafkaListener(
                topics = "retry-test-topic",
                groupId = "retry-test-group",
                containerFactory = "retryTestContainerFactory"
        )
        public void listen(TestEvent event, Acknowledgment acknowledgment) {
            int count = processedCount.incrementAndGet();

            String eventId = event.getId();
            int attemptNum = eventAttempts.computeIfAbsent(eventId, k -> new AtomicInteger(0))
                    .incrementAndGet();

            System.out.println("📝 Processing: " + eventId +
                    " (attempt " + attemptNum + ", total processed: " + count + ")");

            // Check if should succeed on this attempt
            Integer succeedAfter = failUntilAttempt.get(eventId);
            if (succeedAfter != null && attemptNum >= succeedAfter) {
                successfulEvents.add(eventId);
                System.out.println("✅ Event " + eventId + " succeeded on attempt " + attemptNum);
                return;
            }

            if (event.isShouldFail()) {
                System.out.println("❌ Event " + eventId + " failing on attempt " + attemptNum);
                throw new RuntimeException("Simulated failure: " + eventId);
            }

            successfulEvents.add(eventId);
            System.out.println("✅ Event " + eventId + " succeeded immediately");
        }

        public void setFailUntilAttempt(String eventId, int attemptNumber) {
            failUntilAttempt.put(eventId, attemptNumber);
        }

        public int getProcessedCount() {
            return processedCount.get();
        }

        public boolean hasSucceeded(String eventId) {
            return successfulEvents.contains(eventId);
        }

        public void reset() {
            processedCount.set(0);
            successfulEvents.clear();
            failUntilAttempt.clear();
            eventAttempts.clear();
        }
    }

    @Component
    public static class RetryDLQCollector {
        private final List<EventWrapper<?>> receivedMessages = new CopyOnWriteArrayList<>();

        @KafkaListener(
                topics = "retry-test-dlq",
                groupId = "retry-dlq-group",
                containerFactory = "dlqKafkaListenerContainerFactory"
        )
        public void collect(EventWrapper<?> message) {
            System.out.println("📨 DLQ received message (total: " + (receivedMessages.size() + 1) + ")");
            receivedMessages.add(message);
        }

        public List<EventWrapper<?>> getReceivedMessages() {
            return new CopyOnWriteArrayList<>(receivedMessages);
        }

        public void reset() {
            receivedMessages.clear();
        }
    }

    @Configuration
    @EnableKafka
    @EnableAspectJAutoProxy
    public static class TestConfig {

        @Bean
        public RetryDelayCalculator retryDelayCalculator() {
            return new RetryDelayCalculator();
        }

        @Bean
        public KafkaRetryListener kafkaRetryListener(
                KafkaTemplate<String, Object> defaultKafkaTemplate,
                RetryDelayCalculator delayCalculator) {
            return new KafkaRetryListener(defaultKafkaTemplate, delayCalculator);
        }

        @Bean
        public ObjectMapper kafkaObjectMapper() {
            ObjectMapper mapper = new ObjectMapper();
            mapper.registerModule(new JavaTimeModule());
            BasicPolymorphicTypeValidator ptv = BasicPolymorphicTypeValidator.builder()
                    .allowIfBaseType(Object.class)
                    .build();
            mapper.activateDefaultTyping(ptv, ObjectMapper.DefaultTyping.NON_FINAL);
            return mapper;
        }
        @Bean(name = "retryContainerFactory")
        public ConcurrentKafkaListenerContainerFactory<String, EventWrapper<?>> retryContainerFactory(
                EmbeddedKafkaBroker embeddedKafka,
                ObjectMapper kafkaObjectMapper) {

            Map<String, Object> props = new HashMap<>();
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, embeddedKafka.getBrokersAsString());
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

        @Bean
        public ConsumerFactory<String, TestEvent> retryTestConsumerFactory(
                EmbeddedKafkaBroker embeddedKafka,
                ObjectMapper kafkaObjectMapper) {
            Map<String, Object> props = new HashMap<>();
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, embeddedKafka.getBrokersAsString());
            props.put(ConsumerConfig.GROUP_ID_CONFIG, "retry-test-group");
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

            JsonDeserializer<TestEvent> jsonDeserializer =
                    new JsonDeserializer<>(TestEvent.class, kafkaObjectMapper);
            jsonDeserializer.addTrustedPackages("*");
            jsonDeserializer.setUseTypeHeaders(false);

            return new DefaultKafkaConsumerFactory<>(
                    props,
                    new StringDeserializer(),
                    jsonDeserializer
            );
        }

// Inside the TestConfig class, REPLACE the existing retryTestContainerFactory method:

// Inside the TestConfig class, REPLACE the existing retryTestContainerFactory method:

        @Bean(name = "retryTestContainerFactory")
        public ConcurrentKafkaListenerContainerFactory<String, TestEvent> retryTestContainerFactory(
                ConsumerFactory<String, TestEvent> retryTestConsumerFactory) {
            ConcurrentKafkaListenerContainerFactory<String, TestEvent> factory =
                    new ConcurrentKafkaListenerContainerFactory<>();
            factory.setConsumerFactory(retryTestConsumerFactory);

            // ✅ CRITICAL: Completely disable error handling so aspect can handle it
            factory.setCommonErrorHandler(new org.springframework.kafka.listener.CommonErrorHandler() {
                @Override
                public boolean handleOne(Exception thrownException,
                                         org.apache.kafka.clients.consumer.ConsumerRecord<?, ?> record,
                                         org.apache.kafka.clients.consumer.Consumer<?, ?> consumer,
                                         org.springframework.kafka.listener.MessageListenerContainer container) {
                    // Do nothing - let aspect handle it
                    return true; // Mark as handled
                }

                @Override
                public void handleBatch(Exception thrownException,
                                        org.apache.kafka.clients.consumer.ConsumerRecords<?, ?> records,
                                        org.apache.kafka.clients.consumer.Consumer<?, ?> consumer,
                                        org.springframework.kafka.listener.MessageListenerContainer container,
                                        Runnable invokeListener) {
                    // Do nothing - let aspect handle it
                }
            });

            factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
            return factory;
        }

        @Bean
        public ProducerFactory<String, TestEvent> producerFactory(
                EmbeddedKafkaBroker embeddedKafka,
                ObjectMapper kafkaObjectMapper) {
            Map<String, Object> props = new HashMap<>();
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, embeddedKafka.getBrokersAsString());
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

            DefaultKafkaProducerFactory<String, TestEvent> factory =
                    new DefaultKafkaProducerFactory<>(props);
            factory.setValueSerializer(new JsonSerializer<>(kafkaObjectMapper));
            return factory;
        }

        @Bean
        public KafkaTemplate<String, TestEvent> kafkaTemplate(
                ProducerFactory<String, TestEvent> producerFactory) {
            return new KafkaTemplate<>(producerFactory);
        }

        @Bean(name = "defaultKafkaTemplate")
        public KafkaTemplate<String, Object> defaultKafkaTemplate(
                EmbeddedKafkaBroker embeddedKafka,
                ObjectMapper kafkaObjectMapper) {
            Map<String, Object> props = new HashMap<>();
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, embeddedKafka.getBrokersAsString());
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
            DefaultKafkaProducerFactory<String, Object> factory =
                    new DefaultKafkaProducerFactory<>(props);

            JsonSerializer<Object> serializer = new JsonSerializer<>(kafkaObjectMapper);

            serializer.setAddTypeInfo(true);
            factory.setValueSerializer(serializer);

            return new KafkaTemplate<>(factory);
        }

        @Bean(name = "defaultFactory")
        @Primary
        public ConcurrentKafkaListenerContainerFactory<String, Object> defaultFactory(
                EmbeddedKafkaBroker embeddedKafka,
                ObjectMapper kafkaObjectMapper) {

            Map<String, Object> props = new HashMap<>();
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, embeddedKafka.getBrokersAsString());
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
            factory.setCommonErrorHandler(null);
            factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);

            return factory;
        }

        @Bean(name = "dlqConsumerFactory")
        public ConsumerFactory<String, EventWrapper<?>> dlqConsumerFactory(
                EmbeddedKafkaBroker embeddedKafka,
                ObjectMapper kafkaObjectMapper) {
            Map<String, Object> props = new HashMap<>();
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, embeddedKafka.getBrokersAsString());
            props.put(ConsumerConfig.GROUP_ID_CONFIG, "retry-dlq-group");
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

        @Bean(name = "dlqKafkaListenerContainerFactory")
        public ConcurrentKafkaListenerContainerFactory<String, EventWrapper<?>> dlqKafkaListenerContainerFactory(
                ConsumerFactory<String, EventWrapper<?>> dlqConsumerFactory) {
            ConcurrentKafkaListenerContainerFactory<String, EventWrapper<?>> factory =
                    new ConcurrentKafkaListenerContainerFactory<>();
            factory.setConsumerFactory(dlqConsumerFactory);
            return factory;
        }
    }
}