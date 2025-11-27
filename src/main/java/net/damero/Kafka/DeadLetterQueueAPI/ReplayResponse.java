package net.damero.Kafka.DeadLetterQueueAPI;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * Response for DLQ replay operations
 */
@Data
@AllArgsConstructor
public class ReplayResponse {
    private String message;
    private String topic;
    private String timestamp;
}

