package net.damero.Kafka.Aspect.Components.Utility;

/**
 * A wrapper for raw binary data that hasn't been definitively cast to a type
 * yet.
 * Used for 4/8 byte payloads where we need the listener's signature to decide
 * between Integer, Float, Long, or Double.
 */
public class RawBinaryWrapper {
    private final byte[] data;

    public RawBinaryWrapper(byte[] data) {
        this.data = data;
    }

    public byte[] getData() {
        return data;
    }

    @Override
    public String toString() {
        return "RawBinaryWrapper(length=" + (data != null ? data.length : 0) + ")";
    }
}
