package net.damero;

import net.damero.Kafka.Annotations.DameroKafkaListener;
import net.damero.Kafka.Aspect.Components.DLQRouter;
import net.damero.Kafka.Config.DelayMethod;
import net.damero.Kafka.CustomObject.EventMetadata;
import net.damero.Kafka.CustomObject.EventWrapper;
import net.damero.Kafka.KafkaServices.KafkaDLQ;
import net.damero.Kafka.Tracing.TracingService;
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
    private DameroKafkaListener dameroKafkaListener;

    private DLQRouter dlqRouter;

    @Mock
    private TracingService tracingService;

    @BeforeEach
    void setUp() {
        dlqRouter = new DLQRouter(tracingService, kafkaTemplate);
        lenient().when(dameroKafkaListener.topic()).thenReturn("test-topic");
        lenient().when(dameroKafkaListener.dlqTopic()).thenReturn("test-dlq");
        lenient().when(dameroKafkaListener.delay()).thenReturn(1000.0);
        lenient().when(dameroKafkaListener.delayMethod()).thenReturn(DelayMethod.EXPO);
        lenient().when(dameroKafkaListener.maxAttempts()).thenReturn(3);
    }

    @Test
    void testSendToDLQForCircuitBreakerOpen() {
        String originalEvent = "test-event";
        String eventId = "test-event-id";

        try (MockedStatic<KafkaDLQ> kafkaDLQMock = mockStatic(KafkaDLQ.class)) {
            ArgumentCaptor<EventWrapper> wrapperCaptor = ArgumentCaptor.forClass(EventWrapper.class);
            dlqRouter.sendToDLQForCircuitBreakerOpen(kafkaTemplate, originalEvent, dameroKafkaListener, eventId);

            kafkaDLQMock.verify(() -> KafkaDLQ.sendToDLQ(eq(kafkaTemplate), eq("test-dlq"), wrapperCaptor.capture()));
            EventWrapper<?> capturedWrapper = wrapperCaptor.getValue();
            assertNotNull(capturedWrapper);
            assertEquals(originalEvent, capturedWrapper.getEvent());
        }
    }

    @Test
    void testSendToDLQAfterMaxAttempts_DefaultTopic() {
        String originalEvent = "test-event";
        Exception exception = new RuntimeException("Processing failed");
        String eventId = "test-event-id";

        try (MockedStatic<KafkaDLQ> kafkaDLQMock = mockStatic(KafkaDLQ.class)) {
            ArgumentCaptor<String> topicCaptor = ArgumentCaptor.forClass(String.class);
            ArgumentCaptor<EventWrapper> wrapperCaptor = ArgumentCaptor.forClass(EventWrapper.class);

            dlqRouter.sendToDLQAfterMaxAttempts(kafkaTemplate, originalEvent, exception, 3, null, eventId,
                    dameroKafkaListener);

            kafkaDLQMock.verify(
                    () -> KafkaDLQ.sendToDLQ(eq(kafkaTemplate), topicCaptor.capture(), wrapperCaptor.capture()));
            assertEquals("test-dlq", topicCaptor.getValue());
            assertEquals(originalEvent, wrapperCaptor.getValue().getEvent());
        }
    }

    @Test
    void testSendToDLQAfterMaxAttempts_CustomTopic() {
        String originalEvent = "test-event";
        Exception exception = new IllegalArgumentException("Validation failed");
        String eventId = "test-event-id";
        String customDlqTopic = "validation-dlq";

        try (MockedStatic<KafkaDLQ> kafkaDLQMock = mockStatic(KafkaDLQ.class)) {
            dlqRouter.sendToDLQAfterMaxAttempts(kafkaTemplate, originalEvent, exception, 1, null, eventId,
                    customDlqTopic, dameroKafkaListener);
            kafkaDLQMock.verify(() -> KafkaDLQ.sendToDLQ(eq(kafkaTemplate), eq(customDlqTopic), any()));
        }
    }
}
