package net.damero.Kafka.CustomKafkaSetup;


//DELAY METHOD FOR RETRY FUNCTIONALITY
public enum DelayMethod {
    EXPO(1),
    LINEAR(1),
    CUSTOM(1),
    MAX(10);

    public final int amount;

    private DelayMethod(int amount){
        this.amount = amount;
    }
}
