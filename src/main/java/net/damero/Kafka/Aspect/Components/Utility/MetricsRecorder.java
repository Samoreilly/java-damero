package net.damero.Kafka.Aspect.Components.Utility;

/**
 * Interface responsible for recording metrics.
 * Decouples the library from specific metrics implementations like Micrometer.
 */
public interface MetricsRecorder {

    /**
     * Records successful message processing metrics.
     * 
     * @param topic     the Kafka topic
     * @param startTime the start time in milliseconds
     */
    void recordSuccess(String topic, long startTime);

    /**
     * Records failed message processing metrics.
     * 
     * @param topic     the Kafka topic
     * @param exception the exception that occurred
     * @param attempts  the number of attempts
     * @param startTime the start time in milliseconds
     */
    void recordFailure(String topic, Exception exception, int attempts, long startTime);

    /**
     * Checks if metrics recording is available.
     * 
     * @return true if metrics recording is enabled and available
     */
    boolean isAvailable();

    /**
     * Records batch processing metrics.
     *
     * @param topic            the Kafka topic
     * @param batchSize        the number of messages in the batch
     * @param processed        the number of successfully processed messages
     * @param failed           the number of failed messages
     * @param processingTimeMs the total batch processing time in milliseconds
     * @param fixedWindow      whether fixed window mode was used
     */
    void recordBatch(String topic, int batchSize, int processed, int failed,
            long processingTimeMs, boolean fixedWindow);

    /**
     * Records batch window expiry event.
     *
     * @param topic        the Kafka topic
     * @param messageCount the number of messages in the batch when window expired
     */
    void recordBatchWindowExpiry(String topic, long messageCount);

    /**
     * Records batch capacity reached event.
     *
     * @param topic the Kafka topic
     */
    void recordBatchCapacityReached(String topic);

    /**
     * Records batch backpressure event (when messages are rejected due to full
     * batch).
     *
     * @param topic the Kafka topic
     */
    void recordBatchBackpressure(String topic);
}
