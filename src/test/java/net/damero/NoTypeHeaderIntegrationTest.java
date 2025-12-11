package net.damero;

import net.damero.Kafka.Config.CustomKafkaAutoConfiguration;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.junit.jupiter.api.extension.ExtendWith;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test for handling messages from producers that DON'T send type headers.
 * Verifies that the library emits appropriate warnings when misconfigured.
 *
 * This test validates the guard-rail warning system that prevents production
 * deserialization failures.
 */
@ExtendWith(OutputCaptureExtension.class)
public class NoTypeHeaderIntegrationTest {

    @Test
    public void shouldWarnWhenTypeHeadersEnabledWithoutDefaultType(CapturedOutput output) {
        // Create auto-config
        CustomKafkaAutoConfiguration config = new CustomKafkaAutoConfiguration();

        // Set required properties
        setPrivateField(config, "bootstrapServers", "localhost:9092");
        setPrivateField(config, "consumerGroupId", "test-group");
        setPrivateField(config, "autoOffsetReset", "earliest");

        KafkaProperties kafkaProperties = new KafkaProperties();

        // Create factory with type headers enabled BUT no default type
        // This is the production bug scenario
        config.kafkaListenerContainerFactory(
            config.kafkaObjectMapper(),
            kafkaProperties,
            null, // NO default type
            true  // type headers enabled - DANGER!
        );

        // Verify warning was emitted
        String capturedOutput = output.toString();
        assertThat(capturedOutput).contains("CONFIGURATION WARNING");
        assertThat(capturedOutput).contains("kafka.damero.consumer.use-type-headers=true but no default-type set");
        assertThat(capturedOutput).contains("Messages from non-Spring producers");
        assertThat(capturedOutput).contains("will fail with 'No type information in headers'");
    }

    @Test
    public void shouldNotWarnWhenDefaultTypeProvided(CapturedOutput output) {
        CustomKafkaAutoConfiguration config = new CustomKafkaAutoConfiguration();

        setPrivateField(config, "bootstrapServers", "localhost:9092");
        setPrivateField(config, "consumerGroupId", "test-group");
        setPrivateField(config, "autoOffsetReset", "earliest");

        KafkaProperties kafkaProperties = new KafkaProperties();

        // Create factory with type headers AND default type - SAFE
        config.kafkaListenerContainerFactory(
            config.kafkaObjectMapper(),
            kafkaProperties,
            "java.lang.String", // default type provided
            true  // type headers enabled - OK because we have fallback
        );

        // Verify NO warning
        String capturedOutput = output.toString();
        assertThat(capturedOutput).doesNotContain("CONFIGURATION WARNING");
    }

    @Test
    public void shouldNotWarnWhenTypeHeadersDisabled(CapturedOutput output) {
        CustomKafkaAutoConfiguration config = new CustomKafkaAutoConfiguration();

        setPrivateField(config, "bootstrapServers", "localhost:9092");
        setPrivateField(config, "consumerGroupId", "test-group");
        setPrivateField(config, "autoOffsetReset", "earliest");

        KafkaProperties kafkaProperties = new KafkaProperties();

        // Create factory with type headers disabled - SAFE
        config.kafkaListenerContainerFactory(
            config.kafkaObjectMapper(),
            kafkaProperties,
            null,  // no default type
            false  // type headers DISABLED - OK for non-Spring producers
        );

        // Verify NO warning
        String capturedOutput = output.toString();
        assertThat(capturedOutput).doesNotContain("CONFIGURATION WARNING");
    }

    /**
     * Helper method to set private fields for testing.
     */
    private void setPrivateField(Object target, String fieldName, Object value) {
        try {
            java.lang.reflect.Field field = target.getClass().getDeclaredField(fieldName);
            field.setAccessible(true);
            field.set(target, value);
        } catch (Exception e) {
            throw new RuntimeException("Failed to set field: " + fieldName, e);
        }
    }
}

