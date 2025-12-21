package net.damero.Kafka.Aspect.Components.Utility;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import java.util.Map;

public class FlexibleJsonDeserializer implements Deserializer<Object> {

    private final ObjectMapper mapper;
    private final String defaultType;

    public FlexibleJsonDeserializer(ObjectMapper mapper, String defaultType) {
        this.mapper = mapper;
        this.defaultType = defaultType;
    }

    @Override
    public Object deserialize(String topic, byte[] data) {
        // Fallback if headers are not available
        if (data == null) return null;

        try {
            if (defaultType != null && !defaultType.isEmpty()) {
                return mapper.readValue(data, Class.forName(defaultType));
            }
            return mapper.readValue(data, Object.class);
        } catch (Exception e) {
            throw new SerializationException("Failed to deserialize", e);
        }
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {}

    @Override
    public void close() {}
}
