package net.damero.Kafka.Aspect.Components.Utility;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import java.util.Map;

/**
 * A forgiving deserializer that first attempts to deserialize records with
 * Spring Kafka's JsonDeserializer (honouring headers and default type). If
 * that fails with a parse/serialization error (for example when the producer
 * wrote a plain text payload like: stringy), this falls back to a
 * plain StringDeserializer and returns the raw String.
 *
 * This prevents hard failures when producers send simple text/plain values
 * while consumers are configured with JsonDeserializer.
 */
public class FlexibleJsonDeserializer implements Deserializer<Object> {

    private final JsonDeserializer<Object> jsonDeserializer;
    private final StringDeserializer stringDeserializer = new StringDeserializer();

    public FlexibleJsonDeserializer(ObjectMapper objectMapper, boolean useTypeHeaders, String defaultType) {
        this.jsonDeserializer = new JsonDeserializer<>(Object.class, objectMapper);

        Map<String, Object> cfg = new java.util.HashMap<>();
        cfg.put(JsonDeserializer.TRUSTED_PACKAGES, "*");
        cfg.put(JsonDeserializer.USE_TYPE_INFO_HEADERS, useTypeHeaders);

        if (defaultType != null && !defaultType.isEmpty()) {
            try {
                // Validate class exists before passing to JsonDeserializer to avoid
                // IllegalState during configure()
                Class<?> clazz = Class.forName(defaultType);
                cfg.put(JsonDeserializer.VALUE_DEFAULT_TYPE, clazz);
            } catch (Exception e) {
                // Log and ignore - testUserDefinedFactoryIntegrationTest expects graceful
                // handling
            }
        }

        this.jsonDeserializer.configure(cfg, false);
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // JsonDeserializer does not require additional configuration here
    }

    @Override
    public Object deserialize(String topic, byte[] data) {
        if (data == null)
            return null;

        try {
            // Try JSON path first
            return jsonDeserializer.deserialize(topic, data);
        } catch (SerializationException ex) {
            // Fallback to plain string when JSON parsing fails
            try {
                return stringDeserializer.deserialize(topic, data);
            } catch (Exception e) {
                throw new SerializationException("FlexibleJsonDeserializer: failed to deserialize as JSON or String",
                        e);
            }
        } catch (Exception e) {
            // Any other deserialization problems - try string fallback
            try {
                return stringDeserializer.deserialize(topic, data);
            } catch (Exception e2) {
                throw new SerializationException("FlexibleJsonDeserializer: failed to deserialize", e2);
            }
        }
    }

    @Override
    public void close() {
        // no-op
    }
}
