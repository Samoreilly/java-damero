package com.example.kafkaexample;

import com.example.kafkaexample.service.OrderProcessingService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.AbstractMessageListenerContainer;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.test.annotation.DirtiesContext;

import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
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

    @Autowired
    private KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;

    @BeforeEach
    void setUp() throws Exception {
        // Wait for listener containers to be ready
        await().atMost(10, TimeUnit.SECONDS).until(() -> {
            for (MessageListenerContainer container : kafkaListenerEndpointRegistry.getListenerContainers()) {
                if (container instanceof AbstractMessageListenerContainer<?, ?> amlc) {
                    String[] topics = amlc.getContainerProperties().getTopics();
                    if (topics != null) {
                        for (String topic : topics) {
                            if ("orders".equals(topic)) {
                                if (!container.isRunning()) {
                                    container.start();
                                }
                                return container.isRunning();
                            }
                        }
                    }
                }
            }
            return false;
        });
        // Give a small delay to ensure everything is ready
        Thread.sleep(500);
    }

    @Test
    void shouldProcessStringMessageSuccessfully() throws Exception {
        // Given
        String testMessage = "simple-string-order-1-" + System.currentTimeMillis();

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
