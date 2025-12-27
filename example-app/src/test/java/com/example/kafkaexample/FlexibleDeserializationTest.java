package com.example.kafkaexample;

import com.example.kafkaexample.model.OrderEvent;
import com.example.kafkaexample.service.OrderProcessingService;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

@SpringBootTest
@DirtiesContext
@EmbeddedKafka(partitions = 1, topics = { "orders", "test-bool", "test-int", "test-long", "test-double", "test-float" })
class FlexibleDeserializationTest {

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    @SpyBean
    private OrderProcessingService orderProcessingService;

    @Autowired
    private org.springframework.kafka.config.KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;

    @Autowired
    private org.springframework.kafka.test.EmbeddedKafkaBroker embeddedKafkaBroker;

    @org.junit.jupiter.api.BeforeEach
    void setUp() throws Exception {
        // Wait for listener containers to be ready
        org.awaitility.Awaitility.await().atMost(30, TimeUnit.SECONDS).until(() -> {
            for (org.springframework.kafka.listener.MessageListenerContainer container : kafkaListenerEndpointRegistry
                    .getListenerContainers()) {
                if (container instanceof org.springframework.kafka.listener.AbstractMessageListenerContainer<?, ?> amlc) {
                    if (!container.isRunning()) {
                        return false;
                    }
                }
            }
            return true;
        });
        Thread.sleep(1000); // Extra buffer
    }

    @Test
    void shouldProcessOrderEventWith_TypeId_Header() throws Exception {
        // Given
        OrderEvent order = new OrderEvent("order-123", "cust-456", 100.0, "CREDIT_CARD", "NEW");
        ProducerRecord<String, Object> record = new ProducerRecord<>("orders", order);
        record.headers()
                .add(new RecordHeader("__TypeId__", OrderEvent.class.getName().getBytes(StandardCharsets.UTF_8)));

        // When
        kafkaTemplate.send(record).get(5, TimeUnit.SECONDS);

        // Then
        org.mockito.ArgumentCaptor<org.apache.kafka.clients.consumer.ConsumerRecord<String, Object>> captor = org.mockito.ArgumentCaptor
                .forClass(org.apache.kafka.clients.consumer.ConsumerRecord.class);

        verify(orderProcessingService, timeout(10000).atLeastOnce()).processOrder(captor.capture(), any());

        Object value = captor.getValue().value();
        if (value instanceof OrderEvent evt) {
            assert "order-123".equals(evt.getOrderId());
        } else {
            throw new RuntimeException("Expected OrderEvent but got " + value.getClass().getName());
        }
    }

    @Test
    void shouldProcessOrderEventWithout_TypeId_HeaderAsMap() throws Exception {
        // Use manual producer to send String JSON without JsonSerialize adding headers
        try (org.apache.kafka.clients.producer.Producer<String, String> producer = createStringProducer()) {
            String json = "{\"orderId\":\"order-789\",\"customerId\":\"cust-000\",\"amount\":200.0,\"paymentMethod\":\"DEBIT\",\"status\":\"NEW\"}";
            producer.send(new ProducerRecord<>("orders", json)).get();
        }

        // Then
        org.mockito.ArgumentCaptor<org.apache.kafka.clients.consumer.ConsumerRecord<String, Object>> captor = org.mockito.ArgumentCaptor
                .forClass(org.apache.kafka.clients.consumer.ConsumerRecord.class);

        verify(orderProcessingService, timeout(10000).atLeastOnce()).processOrder(captor.capture(), any());

        Object value = captor.getValue().value();
        // Since we sent String JSON but missing Type Header, deserializer sees JSON ->
        // Map
        if (!(value instanceof java.util.Map)) {
            throw new RuntimeException("Expected Map (Zero-Config fallback) but got " + value.getClass().getName());
        }
        java.util.Map<?, ?> map = (java.util.Map<?, ?>) value;
        assert "order-789".equals(map.get("orderId"));
    }

    @Test
    void shouldProcessMalformedJsonAsRawBinaryWrapper() throws Exception {
        try (org.apache.kafka.clients.producer.Producer<String, String> producer = createStringProducer()) {
            String malformedJson = "{broken_json: true";
            producer.send(new ProducerRecord<>("orders", malformedJson)).get();
        }

        // Then
        org.mockito.ArgumentCaptor<org.apache.kafka.clients.consumer.ConsumerRecord<String, Object>> captor = org.mockito.ArgumentCaptor
                .forClass(org.apache.kafka.clients.consumer.ConsumerRecord.class);

        verify(orderProcessingService, timeout(10000).atLeastOnce()).processOrder(captor.capture(), any());

        Object value = captor.getValue().value();
        if (!(value instanceof net.damero.Kafka.Aspect.Components.Utility.RawBinaryWrapper)) {
            throw new RuntimeException(
                    "Expected RawBinaryWrapper for malformed JSON but got " + value.getClass().getName());
        }
    }

    private org.apache.kafka.clients.producer.Producer<String, String> createStringProducer() {
        java.util.Map<String, Object> props = org.springframework.kafka.test.utils.KafkaTestUtils
                .producerProps(embeddedKafkaBroker);
        props.put(org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                org.apache.kafka.common.serialization.StringSerializer.class);
        props.put(org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                org.apache.kafka.common.serialization.StringSerializer.class);
        return new org.apache.kafka.clients.producer.KafkaProducer<>(props);
    }

    // Primitives - using kafkaTemplate is fine if we accept standard serialization,
    // BUT JsonSerializer might add headers.
    // If listeners expect Boolean, and we send Boolean, passing headers is actually
    // GOOD (ideal happy path).
    // The "No Header" test needs Manual Producer.

    @Test
    void shouldProcessBooleanWithoutHeader() throws Exception {
        try (org.apache.kafka.clients.producer.Producer<String, String> producer = createStringProducer()) {
            producer.send(new ProducerRecord<>("test-bool", "true")).get();
        }
        verify(orderProcessingService, timeout(10000)).handleBoolean(eq(true), any());
    }

    @Test
    void shouldProcessIntegerWithoutHeader() throws Exception {
        try (org.apache.kafka.clients.producer.Producer<String, String> producer = createStringProducer()) {
            producer.send(new ProducerRecord<>("test-int", "42")).get();
        }
        verify(orderProcessingService, timeout(10000)).handleInteger(eq(42), any());
    }

    @Test
    void shouldProcessLongWithoutHeader() throws Exception {
        try (org.apache.kafka.clients.producer.Producer<String, String> producer = createStringProducer()) {
            producer.send(new ProducerRecord<>("test-long", "9876543210")).get();
        }
        verify(orderProcessingService, timeout(10000)).handleLong(eq(9876543210L), any());
    }

    @Test
    void shouldProcessDoubleWithoutHeader() throws Exception {
        try (org.apache.kafka.clients.producer.Producer<String, String> producer = createStringProducer()) {
            producer.send(new ProducerRecord<>("test-double", "3.14159")).get();
        }
        verify(orderProcessingService, timeout(10000)).handleDouble(eq(3.14159), any());
    }

    @Test
    void shouldProcessFloatWithoutHeader() throws Exception {
        try (org.apache.kafka.clients.producer.Producer<String, String> producer = createStringProducer()) {
            producer.send(new ProducerRecord<>("test-float", "1.23")).get();
        }

        org.mockito.ArgumentCaptor<Float> captor = org.mockito.ArgumentCaptor.forClass(Float.class);
        verify(orderProcessingService, timeout(10000)).handleFloat(captor.capture(), any());

        if (Math.abs(captor.getValue() - 1.23f) > 0.0001) {
            throw new RuntimeException("Float mismatch");
        }
    }
}
