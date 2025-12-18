package com.example.kafkaexample;

import com.example.kafkaexample.service.OrderProcessingService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;

import java.util.concurrent.TimeUnit;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

@SpringBootTest
@DirtiesContext
@EmbeddedKafka(partitions = 1, topics = { "orders", "test-dlq" })
class KafkaExampleFunctionalTest {

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    @SpyBean
    private OrderProcessingService orderProcessingService;

    @Test
    void shouldProcessStringMessageSuccessfully() throws Exception {
        // Given
        String testMessage = "simple-string-order-1";

        // When
        kafkaTemplate.send("orders", testMessage).get(5, TimeUnit.SECONDS);

        // Then
        // Verify that the listener actually processed the message
        // This confirms deserialization worked (FlexibleJsonDeserializer fell back to
        // String)
        // AND that the aspect didn't crash on String type
        verify(orderProcessingService, timeout(10000).atLeastOnce())
                .processOrder(any(), any());
    }
}
