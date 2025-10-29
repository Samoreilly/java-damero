package net.damero;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.BasicPolymorphicTypeValidator;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import net.damero.Annotations.CustomKafkaListener;
import net.damero.CustomKafkaSetup.CustomKafkaAutoConfiguration;
import net.damero.CustomKafkaSetup.DelayMethod;
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
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.*;
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

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(SpringExtension.class)
@SpringBootTest(
        classes = {
                NonBlockingRetryDelayTest.TestConfig.class,
                CustomKafkaAutoConfiguration.class
        }
)
@EmbeddedKafka(
        partitions = 1,
        topics = {"delay-test-topic", "delay-test-retry", "delay-test-dlq"}
)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class NonBlockingRetryDelayTest {

    @Autowired
    private KafkaTemplate<String, TestEvent> kafkaTemplate;

    @Autowired
    private DelayTestListener delayTestListener;

    @Autowired
    private DelayDLQCollector dlqCollector;

    @Autowired
    private KafkaRetryListener kafkaRetryListener;

    @BeforeEach
    void setUp() {
        delayTestListener.reset();
        dlqCollector.reset();
    }

    @Test
    void testRetryDelayIsNonBlocking() {
        List<TestEvent> events = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            TestEvent event = new TestEvent(UUID.randomUUID().toString(), "Event " + i, true);
            events.add(event);
            kafkaTemplate.send("delay-test-topic", event);
        }

        // All events should be initially processed quickly
        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            assertTrue(delayTestListener.getProcessedCount() >= 5,
                    "All events should be processed initially, got: " + delayTestListener.getProcessedCount());
        });

        // Wait for all retries and DLQ
        await().atMost(60, TimeUnit.SECONDS).untilAsserted(() -> {
            int dlqSize = dlqCollector.getReceivedMessages().size();
            assertEquals(5, dlqSize,
                    "All 5 events should eventually reach DLQ, got: " + dlqSize);
        });
    }

    @Test
    void testRetryDelayIncreasesWithAttempts() {
        TestEvent event = new TestEvent(UUID.randomUUID().toString(), "Delay check", true);

        kafkaTemplate.send("delay-test-topic", event);

        await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> {
            List<Instant> timestamps = delayTestListener.getAttemptTimestamps(event.getId());
            assertTrue(timestamps.size() >= 3,
                    "Should have at least 3 attempts, got: " + timestamps.size());

            if (timestamps.size() >= 3) {
                Duration firstDelay = Duration.between(timestamps.get(0), timestamps.get(1));
                Duration secondDelay = Duration.between(timestamps.get(1), timestamps.get(2));

                System.out.println("First delay: " + firstDelay.toMillis() + "ms");
                System.out.println("Second delay: " + secondDelay.toMillis() + "ms");

                assertTrue(secondDelay.toMillis() >= firstDelay.toMillis() * 0.5,
                        String.format("Second delay (%dms) should be comparable to first (%dms) with exponential backoff",
                                secondDelay.toMillis(), firstDelay.toMillis()));
            }
        });
    }

    @Test
    void testMainThreadIsNotBlocked() {
        TestEvent event = new TestEvent(UUID.randomUUID().toString(), "Non-blocking test", true);

        Instant sendTime = Instant.now();
        kafkaTemplate.send("delay-test-topic", event);

        await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
            assertTrue(delayTestListener.getProcessedCount() >= 1,
                    "Should process at least once");
        });

        Instant firstProcessedTime = Instant.now();
        Duration initialProcessingTime = Duration.between(sendTime, firstProcessedTime);

        assertTrue(initialProcessingTime.toMillis() < 5000,
                "Initial processing should be fast (" + initialProcessingTime.toMillis() +
                        "ms), not blocked by retry delay");
    }

    @Test
    void testSchedulerHandlesConcurrentRetries() {
        int eventCount = 20;
        List<TestEvent> events = new ArrayList<>();

        for (int i = 0; i < eventCount; i++) {
            TestEvent event = new TestEvent(UUID.randomUUID().toString(), "Concurrent " + i, true);
            events.add(event);
            kafkaTemplate.send("delay-test-topic", event);
        }

        await().atMost(15, TimeUnit.SECONDS).untilAsserted(() -> {
            assertTrue(delayTestListener.getProcessedCount() >= eventCount,
                    "All " + eventCount + " events should be processed initially, got: " +
                            delayTestListener.getProcessedCount());
        });

        KafkaRetryListener.SchedulerStats stats = kafkaRetryListener.getStats();
        System.out.println("Scheduler stats after initial processing: " + stats);

        await().atMost(90, TimeUnit.SECONDS)
                .pollInterval(2, TimeUnit.SECONDS)
                .untilAsserted(() -> {
                    int dlqCount = dlqCollector.getReceivedMessages().size();
                    System.out.println("DLQ count: " + dlqCount + "/" + eventCount);
                    assertEquals(eventCount, dlqCount,
                            "All events should eventually reach DLQ");
                });
    }

    @Test
    void testDifferentDelayMethods() {
        TestEvent event = new TestEvent(UUID.randomUUID().toString(), "Delay method test", true);

        kafkaTemplate.send("delay-test-topic", event);

        await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> {
            List<Instant> timestamps = delayTestListener.getAttemptTimestamps(event.getId());
            assertTrue(timestamps.size() >= 3,
                    "Should have at least 3 attempts, got: " + timestamps.size());

            for (int i = 1; i < timestamps.size(); i++) {
                Duration delay = Duration.between(timestamps.get(i-1), timestamps.get(i));
                System.out.println("Delay between attempt " + i + " and " + (i+1) + ": " +
                        delay.toMillis() + "ms");
                assertTrue(delay.toMillis() > 100,
                        "Should have meaningful delay (>100ms) between attempts, got: " +
                                delay.toMillis() + "ms");
            }
        });
    }





    @TestConfiguration
    @EnableKafka
    @EnableAspectJAutoProxy
    @Import({
            DelayTestListener.class,
            DelayDLQCollector.class
    })
    public static class TestConfig {

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
            deserializer.setUseTypeHeaders(true);

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
        public ConsumerFactory<String, TestEvent> delayTestConsumerFactory(
                EmbeddedKafkaBroker embeddedKafka,
                ObjectMapper kafkaObjectMapper) {
            Map<String, Object> props = new HashMap<>();
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, embeddedKafka.getBrokersAsString());
            props.put(ConsumerConfig.GROUP_ID_CONFIG, "delay-test-group");
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


// Inside the TestConfig class, REPLACE the existing delayTestContainerFactory method:

        @Bean(name = "delayTestContainerFactory")
        public ConcurrentKafkaListenerContainerFactory<String, TestEvent> delayTestContainerFactory(
                ConsumerFactory<String, TestEvent> delayTestConsumerFactory) {
            ConcurrentKafkaListenerContainerFactory<String, TestEvent> factory =
                    new ConcurrentKafkaListenerContainerFactory<>();
            factory.setConsumerFactory(delayTestConsumerFactory);

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
            serializer.setAddTypeInfo(true);  // ✅ ADD THIS LINE
            factory.setValueSerializer(serializer);

            return new KafkaTemplate<>(factory);
        }

        // CRITICAL FIX: Mark defaultFactory as @Primary
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
            props.put(ConsumerConfig.GROUP_ID_CONFIG, "delay-dlq-group");
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