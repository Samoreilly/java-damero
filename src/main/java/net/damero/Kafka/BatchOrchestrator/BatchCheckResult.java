package net.damero.Kafka.BatchOrchestrator;

public record BatchCheckResult(BatchStatus status, Object object) {
}
