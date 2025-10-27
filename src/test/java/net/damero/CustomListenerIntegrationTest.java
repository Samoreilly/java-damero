package net.damero;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest(classes = TestApplication.class)
@DirtiesContext
@EmbeddedKafka(
        partitions = 1,
        topics = {"test-topic", "test-dlq"},
        brokerProperties = {
                "listeners=PLAINTEXT://localhost:9092",
                "port=9092"
        }
)
class CustomKafkaListenerIntegrationTest {

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    @Autowired
    private TestKafkaListener testKafkaListener;

    @BeforeEach
    void setUp() {
        testKafkaListener.reset();
    }

    @Test
    void testSuccessfulEventProcessing() {
        // Given
        TestEvent event = new TestEvent(
                UUID.randomUUID().toString(),
                "Test message",
                false // should NOT fail
        );

        // When
        kafkaTemplate.send("test-topic", event);

        // Then
        await()
                .atMost(10, TimeUnit.SECONDS)
                .untilAsserted(() -> {
                    assertEquals(1, testKafkaListener.getSuccessfulEvents().size());
                    assertEquals(event.getId(), testKafkaListener.getSuccessfulEvents().get(0).getId());
                    assertEquals(1, testKafkaListener.getAttemptCount(event.getId()));
                });
    }

    @Test
    void testFailedEventWithRetries() {
        // Given
        TestEvent event = new TestEvent(
                UUID.randomUUID().toString(),
                "Test message that will fail",
                true // should fail
        );

        // When
        kafkaTemplate.send("test-topic", event);

        // Then - verify it was attempted 3 times (maxAttempts)
        await()
                .atMost(15, TimeUnit.SECONDS)
                .untilAsserted(() -> {
                    int attempts = testKafkaListener.getAttemptCount(event.getId());
                    assertEquals(3, attempts, "Should have attempted 3 times");
                    assertTrue(testKafkaListener.getFailedEvents().size() >= 3);
                });
    }

    @Test
    void testMultipleEvents() {
        // Given
        TestEvent successEvent1 = new TestEvent(UUID.randomUUID().toString(), "Success 1", false);
        TestEvent successEvent2 = new TestEvent(UUID.randomUUID().toString(), "Success 2", false);
        TestEvent failEvent = new TestEvent(UUID.randomUUID().toString(), "Fail", true);

        // When
        kafkaTemplate.send("test-topic", successEvent1);
        kafkaTemplate.send("test-topic", successEvent2);
        kafkaTemplate.send("test-topic", failEvent);

        // Then
        await()
                .atMost(20, TimeUnit.SECONDS)
                .untilAsserted(() -> {
                    // Two successful events
                    assertEquals(2, testKafkaListener.getSuccessfulEvents().size());

                    // One failed event (attempted 3 times)
                    assertEquals(3, testKafkaListener.getAttemptCount(failEvent.getId()));
                });
    }

    @Test
    void testAspectIsActive() {
        // This test verifies that the aspect is intercepting the listener
        TestEvent event = new TestEvent(
                UUID.randomUUID().toString(),
                "Aspect test",
                false
        );

        kafkaTemplate.send("test-topic", event);

        await()
                .atMost(10, TimeUnit.SECONDS)
                .untilAsserted(() -> {
                    assertFalse(testKafkaListener.getSuccessfulEvents().isEmpty(),
                            "If aspect is working, event should be processed");
                });
    }
}