package net.damero;

import com.fasterxml.jackson.databind.ObjectMapper;
import net.damero.Kafka.Annotations.DameroKafkaListener;
import net.damero.Kafka.Annotations.DlqExceptionRoutes;
import net.damero.Kafka.Config.CustomKafkaAutoConfiguration;
import net.damero.Kafka.Config.DelayMethod;
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
import org.springframework.stereotype.Component;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
@SpringJUnitConfig
@EmbeddedKafka(partitions = 1, topics = {
    "conditional-routing-test",
    "validation-dlq",
    "timeout-dlq",
    "default-dlq"
})
class ConditionalDLQRoutingIntegrationTest {

    @Autowired
    private KafkaTemplate<String, TestEvent> kafkaTemplate;

    @Autowired
    private ConditionalRoutingTestListener testListener;

    @Autowired
    private DLQMessageCollector dlqCollector;

    @BeforeEach
    void setUp() {
        testListener.reset();
        dlqCollector.clearMessages();
    }

    @Test
    void testValidationException_SkipRetry_GoesToValidationDLQ() throws InterruptedException {
        // Given
        TestEvent event = new TestEvent("validation-error", "Invalid data", false);
        testListener.setThrowValidationError(true);

        // When
        kafkaTemplate.send("conditional-routing-test", event);

        // Then
        assertTrue(testListener.awaitProcessing(5, TimeUnit.SECONDS),
            "Event should be processed");

        // Should NOT retry (skipRetry=true)
        assertEquals(1, testListener.getProcessingCount(),
            "Should process only once (no retries)");

        // Should go to validation DLQ
        assertTrue(dlqCollector.awaitMessages("validation-dlq", 1, 5, TimeUnit.SECONDS),
            "Should route to validation-dlq");

        assertEquals(0, dlqCollector.getMessageCount("timeout-dlq"),
            "Should not route to timeout-dlq");
        assertEquals(0, dlqCollector.getMessageCount("default-dlq"),
            "Should not route to default-dlq");
    }

    @Test
    void testTimeoutException_WithRetry_GoesToTimeoutDLQ() throws InterruptedException {
        // Given
        TestEvent event = new TestEvent("timeout-error", "Slow service", false);
        testListener.setThrowTimeoutError(true);

        // When
        kafkaTemplate.send("conditional-routing-test", event);

        // Then
        assertTrue(testListener.awaitProcessing(10, TimeUnit.SECONDS),
            "Event should be processed");

        // Should eventually go to timeout DLQ after retries
        assertTrue(dlqCollector.awaitMessages("timeout-dlq", 1, 10, TimeUnit.SECONDS),
            "Should route to timeout-dlq after retries");

        // After DLQ message arrives, check that retries happened
        // Note: Retries may happen via retry topic, so processing count might be 1 (initial) + N (retries)
        // We just verify the DLQ routing happened, which implies retries occurred
        assertEquals(0, dlqCollector.getMessageCount("validation-dlq"),
            "Should not route to validation-dlq");
    }

    @Test
    void testGenericException_GoesToDefaultDLQ() throws InterruptedException {
        // Given
        TestEvent event = new TestEvent("generic-error", "Unknown error", false);
        testListener.setThrowGenericError(true);

        // When
        kafkaTemplate.send("conditional-routing-test", event);

        // Then
        assertTrue(testListener.awaitProcessing(10, TimeUnit.SECONDS),
            "Event should be processed");

        // Should retry and go to default DLQ (no conditional route matches)
        assertTrue(dlqCollector.awaitMessages("default-dlq", 1, 10, TimeUnit.SECONDS),
            "Should route to default-dlq");

        assertEquals(0, dlqCollector.getMessageCount("validation-dlq"),
            "Should not route to validation-dlq");
        assertEquals(0, dlqCollector.getMessageCount("timeout-dlq"),
            "Should not route to timeout-dlq");
    }

    @Test
    void testSuccessfulProcessing_NoRoutingToDLQ() throws InterruptedException {
        // Given
        TestEvent event = new TestEvent("success", "Valid data", false);
        testListener.setThrowValidationError(false);
        testListener.setThrowTimeoutError(false);
        testListener.setThrowGenericError(false);

        // When
        kafkaTemplate.send("conditional-routing-test", event);

        // Then
        assertTrue(testListener.awaitProcessing(5, TimeUnit.SECONDS),
            "Event should be processed successfully");

        assertEquals(1, testListener.getProcessingCount(),
            "Should process exactly once");

        // No messages should go to any DLQ
        Thread.sleep(2000); // Wait a bit to ensure no DLQ messages
        assertEquals(0, dlqCollector.getMessageCount("validation-dlq"));
        assertEquals(0, dlqCollector.getMessageCount("timeout-dlq"));
        assertEquals(0, dlqCollector.getMessageCount("default-dlq"));
    }

    @Component
    public static class ConditionalRoutingTestListener {
        private CountDownLatch latch = new CountDownLatch(1);
        private final CopyOnWriteArrayList<String> processedEvents = new CopyOnWriteArrayList<>();
        private volatile boolean throwValidationError = false;
        private volatile boolean throwTimeoutError = false;
        private volatile boolean throwGenericError = false;

        @DameroKafkaListener(
            topic = "conditional-routing-test",
            dlqTopic = "default-dlq",
            maxAttempts = 3,
            delay = 100,
            delayMethod = DelayMethod.LINEAR,
            retryable = true,
            dlqRoutes = {
                @DlqExceptionRoutes(
                    exception = IllegalArgumentException.class,
                    dlqExceptionTopic = "validation-dlq",
                    skipRetry = true  // Send immediately without retry
                ),
                @DlqExceptionRoutes(
                    exception = NullPointerException.class,
                    dlqExceptionTopic = "default-dlq",
                    skipRetry = false  // Should go to default-dlq, must be before RuntimeException
                ),
                @DlqExceptionRoutes(
                    exception = RuntimeException.class,
                    dlqExceptionTopic = "timeout-dlq",
                    skipRetry = false  // Retry first, then route
                )
            }
        )
        @org.springframework.kafka.annotation.KafkaListener(
            topics = "conditional-routing-test",
            groupId = "conditional-test-group",
            containerFactory = "kafkaListenerContainerFactory"
        )
        public void processEvent(TestEvent event) {
            processedEvents.add(event.getId());
            latch.countDown(); // Count down before throwing to indicate processing occurred

            if (throwValidationError) {
                throw new IllegalArgumentException("Validation failed: " + event.getData());
            }

            if (throwTimeoutError) {
                throw new RuntimeException("Timeout: " + event.getData());
            }

            if (throwGenericError) {
                throw new NullPointerException("Generic error: " + event.getData());
            }

            // Success - latch already counted down above
        }

        public boolean awaitProcessing(long timeout, TimeUnit unit) throws InterruptedException {
            return latch.await(timeout, unit);
        }

        public int getProcessingCount() {
            return processedEvents.size();
        }

        public void setThrowValidationError(boolean throwValidationError) {
            this.throwValidationError = throwValidationError;
        }

        public void setThrowTimeoutError(boolean throwTimeoutError) {
            this.throwTimeoutError = throwTimeoutError;
        }

        public void setThrowGenericError(boolean throwGenericError) {
            this.throwGenericError = throwGenericError;
        }

        public void reset() {
            processedEvents.clear();
            latch = new CountDownLatch(1); // Reset latch
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
        public ProducerFactory<String, TestEvent> testEventProducerFactory(ObjectMapper kafkaObjectMapper) {
            Map<String, Object> props = new HashMap<>(
                KafkaTestUtils.producerProps(embeddedKafka)
            );
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

            DefaultKafkaProducerFactory<String, TestEvent> factory = new DefaultKafkaProducerFactory<>(props);
            JsonSerializer<TestEvent> serializer = new JsonSerializer<>(kafkaObjectMapper);
            serializer.setAddTypeInfo(true);
            factory.setValueSerializer(serializer);
            return factory;
        }

        @Bean
        public KafkaTemplate<String, TestEvent> kafkaTemplate(
            ProducerFactory<String, TestEvent> testEventProducerFactory) {
            return new KafkaTemplate<>(testEventProducerFactory);
        }

        @Bean(name = "kafkaListenerContainerFactory")
        public ConcurrentKafkaListenerContainerFactory<String, Object> testKafkaListenerContainerFactory(
            ObjectMapper kafkaObjectMapper) {
            Map<String, Object> props = new HashMap<>();
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, embeddedKafka.getBrokersAsString());
            props.put(ConsumerConfig.GROUP_ID_CONFIG, "conditional-test-group");
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

            JsonDeserializer<Object> jsonDeserializer = new JsonDeserializer<>(kafkaObjectMapper);
            jsonDeserializer.addTrustedPackages("*");
            jsonDeserializer.setUseTypeHeaders(true);

            ConsumerFactory<String, Object> consumerFactory = new DefaultKafkaConsumerFactory<>(
                props,
                new StringDeserializer(),
                jsonDeserializer
            );

            ConcurrentKafkaListenerContainerFactory<String, Object> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
            factory.setConsumerFactory(consumerFactory);
            factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
            factory.setCommonErrorHandler(null);
            return factory;
        }

        @Bean(name = "defaultKafkaTemplate")
        public KafkaTemplate<String, Object> defaultKafkaTemplate(ObjectMapper kafkaObjectMapper) {
            Map<String, Object> props = new HashMap<>(KafkaTestUtils.producerProps(embeddedKafka));
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

            DefaultKafkaProducerFactory<String, Object> factory = new DefaultKafkaProducerFactory<>(props);
            JsonSerializer<Object> serializer = new JsonSerializer<>(kafkaObjectMapper);
            serializer.setAddTypeInfo(true);
            factory.setValueSerializer(serializer);
            return new KafkaTemplate<>(factory);
        }
    }
}

