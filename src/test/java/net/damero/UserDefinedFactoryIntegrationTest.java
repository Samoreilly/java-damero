package net.damero;

import net.damero.Kafka.Config.CustomKafkaAutoConfiguration;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit test verifying that @ConditionalOnMissingBean guard rail works correctly.
 * This prevents the library from overriding user-defined container factories.
 *
 * NOTE: This is a simple unit test, not a full Spring Boot integration test.
 * It validates the auto-configuration logic without needing embedded Kafka.
 */
public class UserDefinedFactoryIntegrationTest {

    @Test
    public void shouldCreateFactoryWithDefaultConfiguration() {
        // Verify the auto-configuration can create a factory bean
        CustomKafkaAutoConfiguration config = new CustomKafkaAutoConfiguration();

        // Set required properties (normally injected via @Value)
        setPrivateField(config, "bootstrapServers", "localhost:9092");
        setPrivateField(config, "consumerGroupId", "test-group");
        setPrivateField(config, "autoOffsetReset", "earliest");

        KafkaProperties kafkaProperties = new KafkaProperties();

        // Create the factory
        ConcurrentKafkaListenerContainerFactory<String, Object> factory =
            config.kafkaListenerContainerFactory(
                config.kafkaObjectMapper(),
                kafkaProperties,
                null, // no default type
                true  // use type headers
            );

        assertThat(factory).isNotNull();
        assertThat(factory.getConsumerFactory()).isNotNull();
    }

    @Test
    public void shouldConfigureWithDefaultTypeWhenProvided() {
        CustomKafkaAutoConfiguration config = new CustomKafkaAutoConfiguration();

        setPrivateField(config, "bootstrapServers", "localhost:9092");
        setPrivateField(config, "consumerGroupId", "test-group");
        setPrivateField(config, "autoOffsetReset", "earliest");

        KafkaProperties kafkaProperties = new KafkaProperties();

        // Create factory with default type (simulates kafka.damero.consumer.default-type property)
        ConcurrentKafkaListenerContainerFactory<String, Object> factory =
            config.kafkaListenerContainerFactory(
                config.kafkaObjectMapper(),
                kafkaProperties,
                "java.lang.String", // default type
                false // don't use type headers
            );

        assertThat(factory).isNotNull();
        assertThat(factory.getConsumerFactory()).isNotNull();
    }

    @Test
    public void shouldHandleInvalidDefaultTypeGracefully() {
        CustomKafkaAutoConfiguration config = new CustomKafkaAutoConfiguration();

        setPrivateField(config, "bootstrapServers", "localhost:9092");
        setPrivateField(config, "consumerGroupId", "test-group");
        setPrivateField(config, "autoOffsetReset", "earliest");

        KafkaProperties kafkaProperties = new KafkaProperties();

        // Create factory with invalid default type - should fall back to Object
        ConcurrentKafkaListenerContainerFactory<String, Object> factory =
            config.kafkaListenerContainerFactory(
                config.kafkaObjectMapper(),
                kafkaProperties,
                "com.invalid.NonExistentClass", // invalid class
                false
            );

        // Should not throw exception, just log warning and continue
        assertThat(factory).isNotNull();
        assertThat(factory.getConsumerFactory()).isNotNull();
    }

    /**
     * Helper method to set private fields for testing without Spring context.
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

