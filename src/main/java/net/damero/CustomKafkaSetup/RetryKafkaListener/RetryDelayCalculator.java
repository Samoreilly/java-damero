package net.damero.CustomKafkaSetup.RetryKafkaListener;

import net.damero.CustomKafkaSetup.DelayMethod;
import org.springframework.stereotype.Component;

import java.util.Random;

/**
 * Calculator for retry delays with different backoff strategies
 */
@Component
public class RetryDelayCalculator {

    private final Random random = new Random();

    /**
     * Calculate delay with jitter to prevent thundering herd problem
     */
    public long calculateDelayWithJitter(DelayMethod method, long baseDelayMs, int attemptNumber) {
        long calculatedDelay = calculateDelay(method, baseDelayMs, attemptNumber);

        // Add jitter: ±25% randomness
        double jitterFactor = 0.75 + (random.nextDouble() * 0.5); // 0.75 to 1.25
        long delayWithJitter = (long) (calculatedDelay * jitterFactor);

        return Math.max(0, delayWithJitter);
    }

    /**
     * Calculate delay without jitter
     */
    public long calculateDelay(DelayMethod method, long baseDelayMs, int attemptNumber) {
        switch (method) {
            case LINEAR:
                return calculateLinearDelay(baseDelayMs, attemptNumber);

            case EXPO:
                return calculateExponentialDelay(baseDelayMs, attemptNumber);

            default:
                return baseDelayMs;
        }
    }

    /**
     * Linear backoff: delay increases linearly with each attempt
     * Formula: baseDelay * attemptNumber
     */
    private long calculateLinearDelay(long baseDelayMs, int attemptNumber) {
        return baseDelayMs * attemptNumber;
    }

    /**
     * Exponential backoff: delay doubles with each attempt
     * Formula: baseDelay * 2^(attemptNumber - 1)
     * Capped at max delay to prevent extremely long waits
     */
    private long calculateExponentialDelay(long baseDelayMs, int attemptNumber) {
        // 2^(attemptNumber - 1)
        long multiplier = (long) Math.pow(2, attemptNumber - 1);

        // Cap the multiplier to prevent overflow and excessive delays
        long maxMultiplier = 32; // Max ~32 seconds for 1 second base
        multiplier = Math.min(multiplier, maxMultiplier);

        long delay = baseDelayMs * multiplier;

        // Cap total delay at 5 minutes
        long maxDelay = 5 * 60 * 1000;
        return Math.min(delay, maxDelay);
    }
}