package net.damero.Kafka.BatchOrchestrator;

public enum BatchStatus {
    /** Still collecting messages, not ready to process */
    PROCESSING,
    /** Batch capacity reached, ready to process */
    CAPACITY_REACHED,
    /** Batch window expired, process whatever is queued */
    WINDOW_EXPIRED,
    /** Batch processing failed */
    FAILED,
}
