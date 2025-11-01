package net.damero;

import net.damero.Kafka.RetryScheduler.RetrySched;
import net.damero.Kafka.Annotations.CustomKafkaListener;
import net.damero.Kafka.Config.DelayMethod;
import net.damero.Kafka.CustomObject.EventMetadata;
import net.damero.Kafka.CustomObject.EventWrapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.TaskScheduler;

import java.time.Instant;
import java.time.LocalDateTime;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class RetrySchedTest {

    @Mock
    private TaskScheduler taskScheduler;

    @Mock
    private KafkaTemplate<String, Object> kafkaTemplate;

    @Mock
    private CustomKafkaListener customKafkaListener;

    private RetrySched retrySched;

    @BeforeEach
    void setUp() {
        retrySched = new RetrySched(taskScheduler);

        // Use lenient() to avoid UnnecessaryStubbingException
        // These stubs are shared across multiple tests
        lenient().when(customKafkaListener.topic()).thenReturn("test-topic");
        lenient().when(customKafkaListener.dlqTopic()).thenReturn("test-dlq");
        lenient().when(customKafkaListener.maxAttempts()).thenReturn(3);
        lenient().when(customKafkaListener.delay()).thenReturn(1000.0);
    }

    @Test
    void testScheduleRetry_LinearDelay() {
        // Given
        when(customKafkaListener.delayMethod()).thenReturn(DelayMethod.LINEAR);

        EventMetadata metadata = createMetadata(1);
        EventWrapper<String> eventWrapper = new EventWrapper<>("test-event", LocalDateTime.now(), metadata);

        // When
        retrySched.scheduleRetry(customKafkaListener, eventWrapper, kafkaTemplate);

        // Then
        ArgumentCaptor<Instant> instantCaptor = ArgumentCaptor.forClass(Instant.class);
        verify(taskScheduler).schedule(any(Runnable.class), instantCaptor.capture());

        Instant scheduledTime = instantCaptor.getValue();
        long delayMs = scheduledTime.toEpochMilli() - Instant.now().toEpochMilli();

        // Linear: delay * attempts = 1000 * 1 = 1000ms
        assertTrue(delayMs >= 900 && delayMs <= 1100, "Delay should be around 1000ms, was: " + delayMs);
    }

    @Test
    void testScheduleRetry_ExponentialDelay() {
        // Given
        when(customKafkaListener.delayMethod()).thenReturn(DelayMethod.EXPO);

        EventMetadata metadata = createMetadata(2);
        EventWrapper<String> eventWrapper = new EventWrapper<>("test-event", LocalDateTime.now(), metadata);

        // When
        retrySched.scheduleRetry(customKafkaListener, eventWrapper, kafkaTemplate);

        // Then
        ArgumentCaptor<Instant> instantCaptor = ArgumentCaptor.forClass(Instant.class);
        verify(taskScheduler).schedule(any(Runnable.class), instantCaptor.capture());

        Instant scheduledTime = instantCaptor.getValue();
        long delayMs = scheduledTime.toEpochMilli() - Instant.now().toEpochMilli();

        // Exponential: base * 2^attempts = 1000 * 2^2 = 4000ms
        assertTrue(delayMs >= 3900 && delayMs <= 4100, "Delay should be around 4000ms, was: " + delayMs);
    }

    @Test
    void testScheduleRetry_ExponentialDelay_MaxCap() {
        // Given
        when(customKafkaListener.delayMethod()).thenReturn(DelayMethod.EXPO);
        when(customKafkaListener.delay()).thenReturn(100.0);

        // Simulate many attempts - should hit 5 second max
        EventMetadata metadata = createMetadata(20);
        EventWrapper<String> eventWrapper = new EventWrapper<>("test-event", LocalDateTime.now(), metadata);

        // When
        retrySched.scheduleRetry(customKafkaListener, eventWrapper, kafkaTemplate);

        // Then
        ArgumentCaptor<Instant> instantCaptor = ArgumentCaptor.forClass(Instant.class);
        verify(taskScheduler).schedule(any(Runnable.class), instantCaptor.capture());

        Instant scheduledTime = instantCaptor.getValue();
        long delayMs = scheduledTime.toEpochMilli() - Instant.now().toEpochMilli();

        // Should be capped at 5000ms
        assertTrue(delayMs <= 5100, "Delay should be capped at 5000ms, was: " + delayMs);
        assertTrue(delayMs >= 4900, "Delay should be at least 4900ms, was: " + delayMs);
    }

    @Test
    void testScheduleRetry_CustomDelay() {
        // Given
        when(customKafkaListener.delayMethod()).thenReturn(DelayMethod.CUSTOM);
        when(customKafkaListener.delay()).thenReturn(2500.0);

        EventMetadata metadata = createMetadata(1);
        EventWrapper<String> eventWrapper = new EventWrapper<>("test-event", LocalDateTime.now(), metadata);

        // When
        retrySched.scheduleRetry(customKafkaListener, eventWrapper, kafkaTemplate);

        // Then
        ArgumentCaptor<Instant> instantCaptor = ArgumentCaptor.forClass(Instant.class);
        verify(taskScheduler).schedule(any(Runnable.class), instantCaptor.capture());

        Instant scheduledTime = instantCaptor.getValue();
        long delayMs = scheduledTime.toEpochMilli() - Instant.now().toEpochMilli();

        // Custom delay: exactly 2500ms
        assertTrue(delayMs >= 2400 && delayMs <= 2600, "Delay should be around 2500ms, was: " + delayMs);
    }

    @Test
    void testExecuteRetry_SendsToCorrectTopic() {
        // Given
        when(customKafkaListener.topic()).thenReturn("test-topic");

        EventMetadata metadata = createMetadata(1);
        EventWrapper<String> eventWrapper = new EventWrapper<>("test-event", LocalDateTime.now(), metadata);

        // When
        retrySched.executeRetry(customKafkaListener, eventWrapper, kafkaTemplate);

        // Then
        verify(kafkaTemplate).send(eq("test-topic"), eq(eventWrapper));
    }

    private EventMetadata createMetadata(int attempts) {
        return new EventMetadata(
                LocalDateTime.now(),
                LocalDateTime.now(),
                new RuntimeException("Test exception"),
                new RuntimeException("Test exception"),
                attempts,
                "test-topic",
                "test-dlq",
                1000L,
                DelayMethod.LINEAR,
                3
        );
    }
}