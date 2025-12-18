package net.damero;

import net.damero.Kafka.Annotations.DameroKafkaListener;
import net.damero.Kafka.Annotations.DlqExceptionRoutes;
import net.damero.Kafka.Aspect.Components.DLQExceptionRoutingManager;
import net.damero.Kafka.Aspect.Components.DLQRouter;
import net.damero.Kafka.Aspect.Components.RetryOrchestrator;
import net.damero.Kafka.Config.DelayMethod;
import net.damero.Kafka.CustomObject.EventMetadata;
import net.damero.Kafka.Tracing.TracingService;
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
    private DameroKafkaListener dameroKafkaListener;
    @Mock
    private DlqExceptionRoutes route1;
    @Mock
    private DlqExceptionRoutes route2;
    @Mock
    private TracingService tracingService;

    private DLQExceptionRoutingManager manager;

    @BeforeEach
    void setUp() {
        manager = new DLQExceptionRoutingManager(dlqRouter, retryOrchestrator, tracingService);
        lenient().when(dameroKafkaListener.topic()).thenReturn("test-topic");
        lenient().when(dameroKafkaListener.dlqTopic()).thenReturn("default-dlq");
        lenient().when(dameroKafkaListener.delay()).thenReturn(1000.0);
        lenient().when(dameroKafkaListener.delayMethod()).thenReturn(DelayMethod.EXPO);
        lenient().when(dameroKafkaListener.maxAttempts()).thenReturn(3);
    }

    @Test
    void testRouteToDLQIfSkipRetry_WithSkipRetryTrue() {
        IllegalArgumentException exception = new IllegalArgumentException("Validation failed");
        String originalEvent = "test-event";
        String eventId = "test-id";

        doReturn(IllegalArgumentException.class).when(route1).exception();
        when(route1.dlqExceptionTopic()).thenReturn("validation-dlq");
        when(route1.skipRetry()).thenReturn(true);
        when(dameroKafkaListener.dlqRoutes()).thenReturn(new DlqExceptionRoutes[] { route1 });

        boolean result = manager.routeToDLQIfSkipRetry(dameroKafkaListener, kafkaTemplate, originalEvent, exception,
                null, eventId);

        assertTrue(result);
        verify(dlqRouter).sendToDLQAfterMaxAttempts(eq(kafkaTemplate), eq(originalEvent), eq(exception), eq(1),
                isNull(), eq(eventId), eq("validation-dlq"), eq(dameroKafkaListener));
    }

    @Test
    void testRouteToDLQAfterMaxAttempts_WithMatchingRoute() {
        RuntimeException exception = new RuntimeException("Timeout");
        String originalEvent = "test-event";
        String eventId = "event-123";

        doReturn(RuntimeException.class).when(route1).exception();
        when(route1.dlqExceptionTopic()).thenReturn("timeout-dlq");
        when(route1.skipRetry()).thenReturn(false);
        when(dameroKafkaListener.dlqRoutes()).thenReturn(new DlqExceptionRoutes[] { route1 });

        manager.routeToDLQAfterMaxAttempts(dameroKafkaListener, kafkaTemplate, originalEvent, exception, eventId, 3,
                null);

        verify(dlqRouter).sendToDLQAfterMaxAttempts(eq(kafkaTemplate), eq(originalEvent), eq(exception), eq(3),
                isNull(), eq(eventId), eq("timeout-dlq"), eq(dameroKafkaListener));
        verify(retryOrchestrator).clearAttempts(eventId);
    }
}
