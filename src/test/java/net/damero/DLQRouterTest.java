package net.damero;

import net.damero.Kafka.Annotations.CustomKafkaListener;
import net.damero.Kafka.Aspect.Components.DLQRouter;
import net.damero.Kafka.Config.DelayMethod;
import net.damero.Kafka.CustomObject.EventMetadata;
import net.damero.Kafka.CustomObject.EventWrapper;
import net.damero.Kafka.KafkaServices.KafkaDLQ;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;

import java.time.LocalDateTime;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class DLQRouterTest {

    @Mock
    private KafkaTemplate<String, Object> kafkaTemplate;

    @Mock
    private CustomKafkaListener customKafkaListener;

    private DLQRouter dlqRouter;

    @BeforeEach
    void setUp() {
        dlqRouter = new DLQRouter();

        // Setup common mocks
        lenient().when(customKafkaListener.topic()).thenReturn("test-topic");
        lenient().when(customKafkaListener.dlqTopic()).thenReturn("test-dlq");
        lenient().when(customKafkaListener.delay()).thenReturn(1000.0);
        lenient().when(customKafkaListener.delayMethod()).thenReturn(DelayMethod.EXPO);
        lenient().when(customKafkaListener.maxAttempts()).thenReturn(3);
    }

    @Test
    void testSendToDLQForCircuitBreakerOpen() {
        // Given
        String originalEvent = "test-event";

        try (MockedStatic<KafkaDLQ> kafkaDLQMock = mockStatic(KafkaDLQ.class)) {
            ArgumentCaptor<EventWrapper> wrapperCaptor = ArgumentCaptor.forClass(EventWrapper.class);

            // When
            dlqRouter.sendToDLQForCircuitBreakerOpen(kafkaTemplate, originalEvent, customKafkaListener);

            // Then
            kafkaDLQMock.verify(() -> KafkaDLQ.sendToDLQ(
                eq(kafkaTemplate),
                eq("test-dlq"),
                wrapperCaptor.capture()
            ));

            EventWrapper<?> capturedWrapper = wrapperCaptor.getValue();
            assertNotNull(capturedWrapper);
            assertEquals(originalEvent, capturedWrapper.getEvent());
            assertNotNull(capturedWrapper.getMetadata());
            assertEquals(1, capturedWrapper.getMetadata().getAttempts());
            assertEquals("Circuit breaker OPEN - service unavailable", 
                capturedWrapper.getMetadata().getLastFailureException().getMessage());
        }
    }

    @Test
    void testSendToDLQAfterMaxAttempts_DefaultTopic() {
        // Given
        String originalEvent = "test-event";
        Exception exception = new RuntimeException("Processing failed");
        int currentAttempts = 3;
        EventMetadata priorMetadata = null;

        try (MockedStatic<KafkaDLQ> kafkaDLQMock = mockStatic(KafkaDLQ.class)) {
            ArgumentCaptor<String> topicCaptor = ArgumentCaptor.forClass(String.class);
            ArgumentCaptor<EventWrapper> wrapperCaptor = ArgumentCaptor.forClass(EventWrapper.class);

            // When
            dlqRouter.sendToDLQAfterMaxAttempts(
                kafkaTemplate,
                originalEvent,
                exception,
                currentAttempts,
                priorMetadata,
                customKafkaListener
            );

            // Then
            kafkaDLQMock.verify(() -> KafkaDLQ.sendToDLQ(
                eq(kafkaTemplate),
                topicCaptor.capture(),
                wrapperCaptor.capture()
            ));

            assertEquals("test-dlq", topicCaptor.getValue());

            EventWrapper<?> capturedWrapper = wrapperCaptor.getValue();
            assertNotNull(capturedWrapper);
            assertEquals(originalEvent, capturedWrapper.getEvent());
            assertEquals(3, capturedWrapper.getMetadata().getAttempts());
            assertEquals(exception, capturedWrapper.getMetadata().getLastFailureException());
            assertEquals("test-dlq", capturedWrapper.getMetadata().getDlqTopic());
        }
    }

    @Test
    void testSendToDLQAfterMaxAttempts_CustomTopic() {
        // Given
        String originalEvent = "test-event";
        Exception exception = new IllegalArgumentException("Validation failed");
        int currentAttempts = 1;
        EventMetadata priorMetadata = null;
        String customDlqTopic = "validation-dlq";

        try (MockedStatic<KafkaDLQ> kafkaDLQMock = mockStatic(KafkaDLQ.class)) {
            ArgumentCaptor<String> topicCaptor = ArgumentCaptor.forClass(String.class);
            ArgumentCaptor<EventWrapper> wrapperCaptor = ArgumentCaptor.forClass(EventWrapper.class);

            // When
            dlqRouter.sendToDLQAfterMaxAttempts(
                kafkaTemplate,
                originalEvent,
                exception,
                currentAttempts,
                priorMetadata,
                customDlqTopic,
                customKafkaListener
            );

            // Then
            kafkaDLQMock.verify(() -> KafkaDLQ.sendToDLQ(
                eq(kafkaTemplate),
                topicCaptor.capture(),
                wrapperCaptor.capture()
            ));

            // Verify custom topic was used
            assertEquals("validation-dlq", topicCaptor.getValue());

            EventWrapper<?> capturedWrapper = wrapperCaptor.getValue();
            assertNotNull(capturedWrapper);
            assertEquals(originalEvent, capturedWrapper.getEvent());
            assertEquals(1, capturedWrapper.getMetadata().getAttempts());
            assertEquals(exception, capturedWrapper.getMetadata().getLastFailureException());
            assertEquals("validation-dlq", capturedWrapper.getMetadata().getDlqTopic());
        }
    }

    @Test
    void testSendToDLQAfterMaxAttempts_PreservesPriorMetadata() {
        // Given
        String originalEvent = "test-event";
        Exception currentException = new RuntimeException("Final failure");
        Exception firstException = new RuntimeException("First failure");
        int currentAttempts = 3;

        LocalDateTime firstFailureTime = LocalDateTime.now().minusMinutes(5);
        EventMetadata priorMetadata = new EventMetadata(
            firstFailureTime,
            LocalDateTime.now().minusSeconds(10),
            firstException,
            new RuntimeException("Second failure"),
            2,
            "test-topic",
            "test-dlq",
            1000L,
            DelayMethod.EXPO,
            3
        );

        try (MockedStatic<KafkaDLQ> kafkaDLQMock = mockStatic(KafkaDLQ.class)) {
            ArgumentCaptor<EventWrapper> wrapperCaptor = ArgumentCaptor.forClass(EventWrapper.class);

            // When
            dlqRouter.sendToDLQAfterMaxAttempts(
                kafkaTemplate,
                originalEvent,
                currentException,
                currentAttempts,
                priorMetadata,
                customKafkaListener
            );

            // Then
            kafkaDLQMock.verify(() -> KafkaDLQ.sendToDLQ(
                any(),
                any(),
                wrapperCaptor.capture()
            ));

            EventWrapper<?> capturedWrapper = wrapperCaptor.getValue();
            EventMetadata metadata = capturedWrapper.getMetadata();

            // Verify first failure metadata is preserved
            assertEquals(firstFailureTime, metadata.getFirstFailureDateTime());
            assertEquals(firstException, metadata.getFirstFailureException());

            // Verify current failure is updated
            assertEquals(currentException, metadata.getLastFailureException());
            assertEquals(3, metadata.getAttempts());
        }
    }

    @Test
    void testSendToDLQAfterMaxAttempts_MultipleCustomTopics() {
        // Test routing different exceptions to different DLQ topics
        String originalEvent = "test-event";

        try (MockedStatic<KafkaDLQ> kafkaDLQMock = mockStatic(KafkaDLQ.class)) {
            ArgumentCaptor<String> topicCaptor = ArgumentCaptor.forClass(String.class);

            // Scenario 1: Validation error -> validation-dlq
            dlqRouter.sendToDLQAfterMaxAttempts(
                kafkaTemplate,
                originalEvent,
                new IllegalArgumentException("Invalid input"),
                1,
                null,
                "validation-dlq",
                customKafkaListener
            );

            // Scenario 2: Timeout error -> timeout-dlq
            dlqRouter.sendToDLQAfterMaxAttempts(
                kafkaTemplate,
                originalEvent,
                new RuntimeException("Timeout"),
                3,
                null,
                "timeout-dlq",
                customKafkaListener
            );

            // Scenario 3: Fraud detection -> fraud-dlq
            dlqRouter.sendToDLQAfterMaxAttempts(
                kafkaTemplate,
                originalEvent,
                new RuntimeException("Fraud detected"),
                1,
                null,
                "fraud-dlq",
                customKafkaListener
            );

            // Then - verify each went to correct DLQ
            kafkaDLQMock.verify(() -> KafkaDLQ.sendToDLQ(
                any(),
                eq("validation-dlq"),
                any()
            ), times(1));

            kafkaDLQMock.verify(() -> KafkaDLQ.sendToDLQ(
                any(),
                eq("timeout-dlq"),
                any()
            ), times(1));

            kafkaDLQMock.verify(() -> KafkaDLQ.sendToDLQ(
                any(),
                eq("fraud-dlq"),
                any()
            ), times(1));
        }
    }

    @Test
    void testSendToDLQAfterMaxAttempts_NullPriorMetadata() {
        // Given
        String originalEvent = "test-event";
        Exception exception = new RuntimeException("Test error");

        try (MockedStatic<KafkaDLQ> kafkaDLQMock = mockStatic(KafkaDLQ.class)) {
            ArgumentCaptor<EventWrapper> wrapperCaptor = ArgumentCaptor.forClass(EventWrapper.class);

            // When - null prior metadata
            dlqRouter.sendToDLQAfterMaxAttempts(
                kafkaTemplate,
                originalEvent,
                exception,
                1,
                null,  // null prior metadata
                "custom-dlq",
                customKafkaListener
            );

            // Then
            kafkaDLQMock.verify(() -> KafkaDLQ.sendToDLQ(
                any(),
                any(),
                wrapperCaptor.capture()
            ));

            EventWrapper<?> capturedWrapper = wrapperCaptor.getValue();
            EventMetadata metadata = capturedWrapper.getMetadata();

            // Verify timestamps are set to current time (not null)
            assertNotNull(metadata.getFirstFailureDateTime());
            assertNotNull(metadata.getLastFailureDateTime());

            // Verify exception is set correctly
            assertEquals(exception, metadata.getFirstFailureException());
            assertEquals(exception, metadata.getLastFailureException());
        }
    }
}

