package net.damero.Kafka.Aspect.Components.Utility;

import com.fasterxml.jackson.databind.ObjectMapper;
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
    public Object deserialize(String topic, org.apache.kafka.common.header.Headers headers, byte[] data) {
        if (data == null)
            return null;

        try {
            // Use readTree to safely inspect the JSON structure without forcing a specific
            // type
            com.fasterxml.jackson.databind.JsonNode tree = mapper.readTree(data);

            // 0. Check if it's an internal EventWrapper JSON
            if (tree.isObject() && tree.has("event") && tree.has("metadata")) {
                try {
                    net.damero.Kafka.CustomObject.EventWrapper<?> wrapper = mapper.treeToValue(tree,
                            net.damero.Kafka.CustomObject.EventWrapper.class);
                    if (wrapper.getMetadata() != null) {
                        net.damero.Kafka.Aspect.Components.Utility.HeaderUtils.writeMetadataToHeaders(headers,
                                wrapper.getMetadata());
                    }
                    return wrapper.getEvent();
                } catch (Exception e) {
                    // If conversion to EventWrapper fails (e.g. incompatible types), fall usage
                    // standard logic
                }
            }

            // 1. Try to find Type Information in Headers (Spring Kafka standard)
            org.apache.kafka.common.header.Header typeHeader = headers.lastHeader("__TypeId__");
            if (typeHeader != null) {
                String typeName = new String(typeHeader.value(), java.nio.charset.StandardCharsets.UTF_8);
                try {
                    return mapper.treeToValue(tree, Class.forName(typeName));
                } catch (ClassNotFoundException e) {
                    // Fallback to defaultType if header class is not found
                }
            }

            // 2. Use defaultType if configured
            if (defaultType != null && !defaultType.trim().isEmpty()) {
                return mapper.treeToValue(tree, Class.forName(defaultType));
            }

            // 3. Last resort: Deserialize as a generic Object (Map, Double, String, etc.)
            return mapper.treeToValue(tree, Object.class);

        } catch (Exception e) {
            // If parsing fails (malformed JSON), return raw bytes as a RawBinaryWrapper
            return new net.damero.Kafka.Aspect.Components.Utility.RawBinaryWrapper(data);
        }
    }

    @Override
    public Object deserialize(String topic, byte[] data) {
        return deserialize(topic, new org.apache.kafka.common.header.internals.RecordHeaders(), data);
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public void close() {
    }
}
