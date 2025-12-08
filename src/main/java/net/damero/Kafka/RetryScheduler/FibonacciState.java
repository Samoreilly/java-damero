package net.damero.Kafka.RetryScheduler;

public class FibonacciState {
    private final long[] sequence;
    private int nextIndex;
    private final int maxIndex;

    public FibonacciState(int limit) {
        if (limit < 2) {
            throw new IllegalArgumentException("Fibonacci limit must be at least 2");
        }
        this.sequence = new long[limit];
        this.sequence[0] = 0;
        this.sequence[1] = 1;
        this.nextIndex = 2;
        this.maxIndex = limit - 1;
    }

    public synchronized long getNext() {
        // If we haven't filled the array yet, calculate next value
        if (nextIndex <= maxIndex) {
            //dp[i] = dp[i-1] + dp[i-2]
            sequence[nextIndex] = sequence[nextIndex - 1] + sequence[nextIndex - 2];
            return sequence[nextIndex++];
        }
        // If at limit, return the last (max) value
        return sequence[maxIndex];
    }

    public synchronized long getCurrent() {
        int currentIndex = Math.max(0, nextIndex - 1);
        return sequence[currentIndex];
    }
}