package net.damero;

import net.damero.Kafka.Annotations.CustomKafkaListener;
import net.damero.Kafka.Annotations.DlqExceptionRoutes;
import net.damero.Kafka.Aspect.Components.DLQExceptionRoutingManager;
import net.damero.Kafka.Aspect.Components.DLQRouter;
import net.damero.Kafka.Aspect.Components.RetryOrchestrator;
import net.damero.Kafka.Config.DelayMethod;
import net.damero.Kafka.CustomObject.EventMetadata;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;

import java.time.LocalDateTime;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class DLQExceptionRoutingManagerTest {

    @Mock
    private DLQRouter dlqRouter;

    @Mock
    private RetryOrchestrator retryOrchestrator;

    @Mock
    private KafkaTemplate<String, Object> kafkaTemplate;

    @Mock
    private CustomKafkaListener customKafkaListener;

    @Mock
    private DlqExceptionRoutes route1;

    @Mock
    private DlqExceptionRoutes route2;

    private DLQExceptionRoutingManager manager;

    @BeforeEach
    void setUp() {
        manager = new DLQExceptionRoutingManager(dlqRouter, retryOrchestrator);

        // Setup common listener mocks
        lenient().when(customKafkaListener.topic()).thenReturn("test-topic");
        lenient().when(customKafkaListener.dlqTopic()).thenReturn("default-dlq");
        lenient().when(customKafkaListener.delay()).thenReturn(1000.0);
        lenient().when(customKafkaListener.delayMethod()).thenReturn(DelayMethod.EXPO);
        lenient().when(customKafkaListener.maxAttempts()).thenReturn(3);
    }

    @Test
    void testRouteToDLQIfSkipRetry_WithSkipRetryTrue() {
        // Given
        IllegalArgumentException exception = new IllegalArgumentException("Validation failed");
        String originalEvent = "test-event";
        EventMetadata existingMetadata = null;

        doReturn(IllegalArgumentException.class).when(route1).exception();
        when(route1.dlqExceptionTopic()).thenReturn("validation-dlq");
        when(route1.skipRetry()).thenReturn(true);

        when(customKafkaListener.dlqRoutes()).thenReturn(new DlqExceptionRoutes[]{route1});

        // When
        boolean result = manager.routeToDLQIfSkipRetry(
            customKafkaListener, kafkaTemplate, originalEvent, exception, existingMetadata);

        // Then
        assertTrue(result, "Should return true when skipRetry=true");

        verify(dlqRouter).sendToDLQAfterMaxAttempts(
            eq(kafkaTemplate),
            eq(originalEvent),
            eq(exception),
            eq(1),
            eq(existingMetadata),
            eq("validation-dlq"),
            eq(customKafkaListener)
        );
    }

    @Test
    void testRouteToDLQIfSkipRetry_WithSkipRetryFalse() {
        // Given
        RuntimeException exception = new RuntimeException("Timeout");
        String originalEvent = "test-event";

        doReturn(RuntimeException.class).when(route1).exception();
        when(route1.dlqExceptionTopic()).thenReturn("timeout-dlq");
        when(route1.skipRetry()).thenReturn(false);

        when(customKafkaListener.dlqRoutes()).thenReturn(new DlqExceptionRoutes[]{route1});

        // When
        boolean result = manager.routeToDLQIfSkipRetry(
            customKafkaListener, kafkaTemplate, originalEvent, exception, null);

        // Then
        assertFalse(result, "Should return false when skipRetry=false");

        // Should not send to DLQ
        verify(dlqRouter, never()).sendToDLQAfterMaxAttempts(any(), any(), any(), anyInt(), any(), any(), any());
    }

    @Test
    void testRouteToDLQIfSkipRetry_NoMatchingRoute() {
        // Given
        NullPointerException exception = new NullPointerException("NPE");
        String originalEvent = "test-event";

        lenient().doReturn(IllegalArgumentException.class).when(route1).exception();
        lenient().when(route1.dlqExceptionTopic()).thenReturn("validation-dlq");
        lenient().when(route1.skipRetry()).thenReturn(true);

        when(customKafkaListener.dlqRoutes()).thenReturn(new DlqExceptionRoutes[]{route1});

        // When
        boolean result = manager.routeToDLQIfSkipRetry(
            customKafkaListener, kafkaTemplate, originalEvent, exception, null);

        // Then
        assertFalse(result, "Should return false when no route matches");

        verify(dlqRouter, never()).sendToDLQAfterMaxAttempts(any(), any(), any(), anyInt(), any(), any(), any());
    }

    @Test
    void testRouteToDLQIfSkipRetry_NoRoutesConfigured() {
        // Given
        RuntimeException exception = new RuntimeException("Error");
        String originalEvent = "test-event";

        when(customKafkaListener.dlqRoutes()).thenReturn(new DlqExceptionRoutes[]{});

        // When
        boolean result = manager.routeToDLQIfSkipRetry(
            customKafkaListener, kafkaTemplate, originalEvent, exception, null);

        // Then
        assertFalse(result, "Should return false when no routes configured");

        verify(dlqRouter, never()).sendToDLQAfterMaxAttempts(any(), any(), any(), anyInt(), any(), any(), any());
    }

    @Test
    void testRouteToDLQIfSkipRetry_ExceptionInheritance() {
        // Given - route matches parent exception class
        IllegalArgumentException exception = new IllegalArgumentException("Validation failed");
        String originalEvent = "test-event";

        doReturn(RuntimeException.class).when(route1).exception();  // Parent class
        when(route1.dlqExceptionTopic()).thenReturn("error-dlq");
        when(route1.skipRetry()).thenReturn(true);

        when(customKafkaListener.dlqRoutes()).thenReturn(new DlqExceptionRoutes[]{route1});

        // When
        boolean result = manager.routeToDLQIfSkipRetry(
            customKafkaListener, kafkaTemplate, originalEvent, exception, null);

        // Then
        assertTrue(result, "Should match parent exception class");

        verify(dlqRouter).sendToDLQAfterMaxAttempts(
            any(), any(), any(), anyInt(), any(), eq("error-dlq"), any());
    }

    @Test
    void testRouteToDLQAfterMaxAttempts_WithMatchingRouteSkipRetryFalse() {
        // Given
        RuntimeException exception = new RuntimeException("Timeout");
        String originalEvent = "test-event";
        String eventId = "event-123";
        int currentAttempts = 3;
        EventMetadata metadata = createTestMetadata();

        doReturn(RuntimeException.class).when(route1).exception();
        when(route1.dlqExceptionTopic()).thenReturn("timeout-dlq");
        when(route1.skipRetry()).thenReturn(false);

        when(customKafkaListener.dlqRoutes()).thenReturn(new DlqExceptionRoutes[]{route1});

        // When
        manager.routeToDLQAfterMaxAttempts(
            customKafkaListener, kafkaTemplate, originalEvent, exception, eventId, currentAttempts, metadata);

        // Then
        verify(dlqRouter).sendToDLQAfterMaxAttempts(
            eq(kafkaTemplate),
            eq(originalEvent),
            eq(exception),
            eq(currentAttempts),
            eq(metadata),
            eq("timeout-dlq"),  // Should use conditional DLQ
            eq(customKafkaListener)
        );

        verify(retryOrchestrator).clearAttempts(eventId);
    }

    @Test
    void testRouteToDLQAfterMaxAttempts_FallbackToDefaultDLQ() {
        // Given - no matching route
        NullPointerException exception = new NullPointerException("NPE");
        String originalEvent = "test-event";
        String eventId = "event-123";
        int currentAttempts = 3;

        lenient().doReturn(IllegalArgumentException.class).when(route1).exception();
        lenient().when(route1.dlqExceptionTopic()).thenReturn("validation-dlq");
        lenient().when(route1.skipRetry()).thenReturn(false);

        when(customKafkaListener.dlqRoutes()).thenReturn(new DlqExceptionRoutes[]{route1});

        // When
        manager.routeToDLQAfterMaxAttempts(
            customKafkaListener, kafkaTemplate, originalEvent, exception, eventId, currentAttempts, null);

        // Then - should fallback to default DLQ
        verify(dlqRouter).sendToDLQAfterMaxAttempts(
            eq(kafkaTemplate),
            eq(originalEvent),
            eq(exception),
            eq(currentAttempts),
            isNull(),
            eq("default-dlq"),  // Should use default DLQ
            eq(customKafkaListener)
        );

        verify(retryOrchestrator).clearAttempts(eventId);
    }

    @Test
    void testRouteToDLQAfterMaxAttempts_SkipsRouteWithSkipRetryTrue() {
        // Given - route has skipRetry=true, should fallback to default
        IllegalArgumentException exception = new IllegalArgumentException("Validation");
        String originalEvent = "test-event";
        String eventId = "event-123";
        int currentAttempts = 3;

        doReturn(IllegalArgumentException.class).when(route1).exception();
        when(route1.dlqExceptionTopic()).thenReturn("validation-dlq");
        when(route1.skipRetry()).thenReturn(true);  // This route should be skipped

        when(customKafkaListener.dlqRoutes()).thenReturn(new DlqExceptionRoutes[]{route1});

        // When
        manager.routeToDLQAfterMaxAttempts(
            customKafkaListener, kafkaTemplate, originalEvent, exception, eventId, currentAttempts, null);

        // Then - should fallback to default DLQ (skipRetry=true routes only apply in routeToDLQIfSkipRetry)
        verify(dlqRouter).sendToDLQAfterMaxAttempts(
            any(), any(), any(), anyInt(), any(), eq("default-dlq"), any());

        verify(retryOrchestrator).clearAttempts(eventId);
    }

    @Test
    void testRouteToDLQAfterMaxAttempts_MultipleRoutes_FirstMatchWins() {
        // Given - multiple routes, first match should win
        RuntimeException exception = new RuntimeException("Error");
        String originalEvent = "test-event";
        String eventId = "event-123";

        doReturn(RuntimeException.class).when(route1).exception();
        when(route1.dlqExceptionTopic()).thenReturn("first-dlq");
        when(route1.skipRetry()).thenReturn(false);

        lenient().doReturn(RuntimeException.class).when(route2).exception();
        lenient().when(route2.dlqExceptionTopic()).thenReturn("second-dlq");
        lenient().when(route2.skipRetry()).thenReturn(false);

        when(customKafkaListener.dlqRoutes()).thenReturn(new DlqExceptionRoutes[]{route1, route2});

        // When
        manager.routeToDLQAfterMaxAttempts(
            customKafkaListener, kafkaTemplate, originalEvent, exception, eventId, 3, null);

        // Then - should use first matching route
        verify(dlqRouter).sendToDLQAfterMaxAttempts(
            any(), any(), any(), anyInt(), any(), eq("first-dlq"), any());
    }

    @Test
    void testRouteToDLQIfSkipRetry_EmptyDlqTopic() {
        // Given - route with empty DLQ topic
        IllegalArgumentException exception = new IllegalArgumentException("Error");
        String originalEvent = "test-event";

        lenient().doReturn(IllegalArgumentException.class).when(route1).exception();
        when(route1.dlqExceptionTopic()).thenReturn("");  // Empty topic
        lenient().when(route1.skipRetry()).thenReturn(true);

        when(customKafkaListener.dlqRoutes()).thenReturn(new DlqExceptionRoutes[]{route1});

        // When
        boolean result = manager.routeToDLQIfSkipRetry(
            customKafkaListener, kafkaTemplate, originalEvent, exception, null);

        // Then - should skip invalid route
        assertFalse(result, "Should return false for route with empty DLQ topic");

        verify(dlqRouter, never()).sendToDLQAfterMaxAttempts(any(), any(), any(), anyInt(), any(), any(), any());
    }

    @Test
    void testRouteToDLQIfSkipRetry_NullExceptionClass() {
        // Given - route with null exception class
        IllegalArgumentException exception = new IllegalArgumentException("Error");
        String originalEvent = "test-event";

        when(route1.exception()).thenReturn(null);  // Null exception
        lenient().when(route1.dlqExceptionTopic()).thenReturn("validation-dlq");
        lenient().when(route1.skipRetry()).thenReturn(true);

        when(customKafkaListener.dlqRoutes()).thenReturn(new DlqExceptionRoutes[]{route1});

        // When
        boolean result = manager.routeToDLQIfSkipRetry(
            customKafkaListener, kafkaTemplate, originalEvent, exception, null);

        // Then - should skip invalid route
        assertFalse(result, "Should return false for route with null exception class");

        verify(dlqRouter, never()).sendToDLQAfterMaxAttempts(any(), any(), any(), anyInt(), any(), any(), any());
    }

    @Test
    void testRouteToDLQIfSkipRetry_MultipleRoutes_SecondMatches() {
        // Given - first route doesn't match, second does
        IllegalArgumentException exception = new IllegalArgumentException("Validation");
        String originalEvent = "test-event";

        lenient().doReturn(NullPointerException.class).when(route1).exception();
        lenient().when(route1.dlqExceptionTopic()).thenReturn("npe-dlq");
        lenient().when(route1.skipRetry()).thenReturn(true);

        doReturn(IllegalArgumentException.class).when(route2).exception();
        when(route2.dlqExceptionTopic()).thenReturn("validation-dlq");
        when(route2.skipRetry()).thenReturn(true);

        when(customKafkaListener.dlqRoutes()).thenReturn(new DlqExceptionRoutes[]{route1, route2});

        // When
        boolean result = manager.routeToDLQIfSkipRetry(
            customKafkaListener, kafkaTemplate, originalEvent, exception, null);

        // Then
        assertTrue(result);

        verify(dlqRouter).sendToDLQAfterMaxAttempts(
            any(), any(), any(), anyInt(), any(), eq("validation-dlq"), any());
    }

    private EventMetadata createTestMetadata() {
        return new EventMetadata(
            LocalDateTime.now().minusMinutes(5),
            LocalDateTime.now(),
            new RuntimeException("First error"),
            new RuntimeException("Last error"),
            2,
            "test-topic",
            "test-dlq",
            1000L,
            DelayMethod.EXPO,
            3
        );
    }
}

