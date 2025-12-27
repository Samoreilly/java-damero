package net.damero.Kafka.Config;

//DELAY METHOD FOR RETRY FUNCTIONALITY
public enum DelayMethod {
    EXPO(1),
    LINEAR(1),
    FIBONACCI(10), // MAX DELAY
    CUSTOM(1),
    MAX(10),
    JITTER(1);

    public final int amount;

    private DelayMethod(int amount) {
        this.amount = amount;
    }
}
