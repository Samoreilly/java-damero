package net.damero.Kafka.Config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.BasicPolymorphicTypeValidator;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import net.damero.Kafka.Aspect.Components.*;
import net.damero.Kafka.Aspect.Deduplication.DuplicationManager;
import net.damero.Kafka.Aspect.KafkaListenerAspect;
import net.damero.Kafka.CustomObject.EventWrapper;
import net.damero.Kafka.DeadLetterQueueAPI.DLQController;
import net.damero.Kafka.DeadLetterQueueAPI.ReadFromDLQ.ReadFromDLQConsumer;
import net.damero.Kafka.KafkaServices.KafkaDLQ;
import org.springframework.context.ApplicationContext;
import net.damero.Kafka.Resilience.CircuitBreakerService;
import net.damero.Kafka.RetryScheduler.RetrySched;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.AutoConfigureBefore;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication;
import org.springframework.boot.autoconfigure.jackson.JacksonAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.EnableAspectJAutoProxy;
import org.springframework.context.annotation.Primary;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.lang.Nullable;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

/**
 * Auto-configuration for CustomKafka library.
 * Provides default beans that users can override if needed.
 *
 * Can be disabled by setting: custom.kafka.auto-config.enabled=false
 */
@AutoConfiguration
@AutoConfigureBefore(JacksonAutoConfiguration.class)
@EnableAspectJAutoProxy
@ConditionalOnProperty(
        prefix = "custom.kafka.auto-config",
        name = "enabled",
        havingValue = "true",
        matchIfMissing = true
)
public class CustomKafkaAutoConfiguration {

    @Value("${spring.kafka.bootstrap-servers:localhost:9092}")
    private String bootstrapServers;

    @Bean
    @ConditionalOnMissingBean
    public KafkaDLQ kafkaDLQ() {
        return new KafkaDLQ();
    }

    @Bean
    @ConditionalOnMissingBean
    public ReadFromDLQConsumer readFromDLQConsumer(ConsumerFactory<String, EventWrapper<?>> dlqConsumerFactory,
                                                   ObjectMapper kafkaObjectMapper) {
        return new ReadFromDLQConsumer(dlqConsumerFactory, kafkaObjectMapper);
    }

    @Bean
    @ConditionalOnMissingBean
    public DLQRouter dlqRouter() {
        return new DLQRouter();
    }

    @Bean
    @ConditionalOnMissingBean
    public CaffeineCache caffeineCache() {
        return new CaffeineCache();
    }

    @Bean
    @ConditionalOnMissingBean(DLQController.class)
    public DLQController dlqController(ReadFromDLQConsumer readFromDLQConsumer) {
        return new DLQController(readFromDLQConsumer);
    }

    @Bean
    @ConditionalOnMissingBean
    public RetryOrchestrator retryOrchestrator(RetrySched retrySched, CaffeineCache caffeineCache) {
        return new RetryOrchestrator(retrySched, caffeineCache);
    }

    @Bean
    @ConditionalOnMissingBean
    public DLQExceptionRoutingManager dlqExceptionRoutingManager(DLQRouter dlqRouter, RetryOrchestrator retryOrchestrator){
        return new DLQExceptionRoutingManager(dlqRouter, retryOrchestrator);
    }

    @Bean
    @ConditionalOnMissingBean
    public MetricsRecorder metricsRecorder(@Nullable io.micrometer.core.instrument.MeterRegistry meterRegistry) {
        return new MetricsRecorder(meterRegistry);
    }

    @Bean
    @ConditionalOnMissingBean
    public CircuitBreakerWrapper circuitBreakerWrapper(@Nullable CircuitBreakerService circuitBreakerService) {
        return new CircuitBreakerWrapper(circuitBreakerService);
    }

    @Bean
    @ConditionalOnMissingBean
    public KafkaListenerAspect kafkaListenerAspect(DLQRouter dlqRouter,
                                                   ApplicationContext context,
                                                   KafkaTemplate<?, ?> defaultKafkaTemplate,
                                                   RetryOrchestrator retryOrchestrator,
                                                   MetricsRecorder metricsRecorder,
                                                   CircuitBreakerWrapper circuitBreakerWrapper,
                                                   RetrySched retrySched,
                                                   DLQExceptionRoutingManager dlqExceptionRoutingManager,
                                                   DuplicationManager duplicationManager) {
        return new KafkaListenerAspect(dlqRouter, context, defaultKafkaTemplate, 
                                       retryOrchestrator, metricsRecorder, circuitBreakerWrapper, retrySched, dlqExceptionRoutingManager, duplicationManager);
    }

    /*
     * TaskScheduler for retrying failed messages
     */
    @Bean
    @ConditionalOnMissingBean(name = "kafkaRetryScheduler")
    public TaskScheduler kafkaRetryScheduler() {
        ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
        scheduler.setPoolSize(20);
        scheduler.setThreadNamePrefix("kafka-retry-scheduler-");
        // Don't wait for tasks to complete on shutdown - interrupt them instead
        // This prevents test hangs when embedded Kafka shuts down before scheduled retries execute
        // In production, this is acceptable as retries will be rescheduled if the application restarts
        scheduler.setWaitForTasksToCompleteOnShutdown(false);
        scheduler.setAwaitTerminationSeconds(1);
        scheduler.setErrorHandler(t ->
                org.slf4j.LoggerFactory.getLogger("kafka-retry-scheduler")
                    .error("scheduler error: {}", t.getMessage(), t)
        );
        scheduler.initialize();
        return scheduler;
    }

    @Bean
    @ConditionalOnMissingBean
    public RetrySched retrySched(TaskScheduler kafkaRetryScheduler) {
        return new RetrySched(kafkaRetryScheduler);
    }

    /*
     Circuit Breaker Registry for Resilience4j (optional) for the user
     */
    @Bean
    @ConditionalOnMissingBean(name = "circuitBreakerRegistry")
    @ConditionalOnClass(name = "io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry")
    public Object circuitBreakerRegistry() {
        try {
            Class<?> registryClass = Class.forName("io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry");
            Method ofDefaultsMethod = registryClass.getMethod("ofDefaults");
            return ofDefaultsMethod.invoke(null);
        } catch (Exception e) {
            // If Resilience4j is not available, return null
            // This bean won't be created due to @ConditionalOnClass
            return null;
        }
    }

    /**
     * Provides a pre-configured ObjectMapper for Kafka serialization.
     * This ObjectMapper has polymorphic typing enabled for EventWrapper support.
     * Users can override this bean to customize serialization behavior.
     * 
     * This bean is NOT @Primary, so Spring Boot will use its own ObjectMapper
     * (or the one we provide below) for REST controllers.
     */
    @Bean(name = "kafkaObjectMapper")
    @ConditionalOnMissingBean(name = "kafkaObjectMapper")
    public ObjectMapper kafkaObjectMapper() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());

        //enable polymorphic type handling for EventWrapper
        BasicPolymorphicTypeValidator ptv = BasicPolymorphicTypeValidator.builder()
                .allowIfBaseType(Object.class)
                .build();
        mapper.activateDefaultTyping(ptv, ObjectMapper.DefaultTyping.NON_FINAL);

        return mapper;
    }

    /**
     * Provides a standard ObjectMapper for REST controllers if Spring Boot hasn't created one.
     * This ensures REST JSON serialization/deserialization works without polymorphic typing,
     * preventing conflicts with the kafkaObjectMapper which has polymorphic typing enabled.
     * 
     * Users can override this bean if they need custom REST JSON configuration.
     */
    @Bean(name = "objectMapper")
    @Primary
    @ConditionalOnMissingBean(name = "objectMapper")
    public ObjectMapper objectMapper() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
        // Explicitly do NOT enable polymorphic typing - just plain JSON for REST
        return mapper;
    }

    /**
     * Configures HttpMessageConverters to use the @Primary ObjectMapper (without polymorphic typing)
     * for REST controllers, ensuring that REST JSON serialization/deserialization works correctly
     * even if the kafkaObjectMapper exists.
     * 
     * Uses extendMessageConverters so it runs after Spring Boot's Jackson auto-configuration,
     * allowing us to replace any MappingJackson2HttpMessageConverter that was created with
     * the wrong ObjectMapper. Spring will automatically inject the @Primary ObjectMapper.
     */
    @Bean
    @ConditionalOnWebApplication(type = ConditionalOnWebApplication.Type.SERVLET)
    @ConditionalOnClass(WebMvcConfigurer.class)
    @ConditionalOnMissingBean(name = "customKafkaWebMvcConfigurer")
    public WebMvcConfigurer customKafkaWebMvcConfigurer(ObjectMapper objectMapper) {
        return new WebMvcConfigurer() {
            @Override
            public void extendMessageConverters(java.util.List<org.springframework.http.converter.HttpMessageConverter<?>> converters) {
                // Remove any existing MappingJackson2HttpMessageConverter and add ours with @Primary ObjectMapper
                converters.removeIf(converter -> converter instanceof MappingJackson2HttpMessageConverter);
                MappingJackson2HttpMessageConverter jsonConverter = new MappingJackson2HttpMessageConverter(objectMapper);
                // Add at the beginning to ensure it's used first
                converters.add(0, jsonConverter);
            }
        };
    }


    /**
     * Default KafkaTemplate for sending to DLQ and other internal operations.
     * Users typically don't need to override this.
     */
    @Bean
    @ConditionalOnMissingBean(name = "defaultKafkaTemplate")
    public KafkaTemplate<String, Object> defaultKafkaTemplate(ObjectMapper kafkaObjectMapper) {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

        DefaultKafkaProducerFactory<String, Object> factory = new DefaultKafkaProducerFactory<>(props);
        JsonSerializer<Object> serializer = new JsonSerializer<>(kafkaObjectMapper);
        serializer.setAddTypeInfo(true);
        factory.setValueSerializer(serializer);

        return new KafkaTemplate<>(factory);
    }

    /**
     * Generic KafkaTemplate for user applications to send messages.
     * This is the same as defaultKafkaTemplate but with a generic name that users can inject.
     */
    @Bean
    @ConditionalOnMissingBean(name = "kafkaTemplate")
    public KafkaTemplate<String, Object> kafkaTemplate(ObjectMapper kafkaObjectMapper) {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

        DefaultKafkaProducerFactory<String, Object> factory = new DefaultKafkaProducerFactory<>(props);
        JsonSerializer<Object> serializer = new JsonSerializer<>(kafkaObjectMapper);
        serializer.setAddTypeInfo(true);
        factory.setValueSerializer(serializer);

        return new KafkaTemplate<>(factory);
    }

    /**
     * Default factory for internal use by the library.
     * Users typically don't need to interact with this.
     */
    @Bean
    @ConditionalOnMissingBean(name = "kafkaListenerContainerFactory")
    public ConcurrentKafkaListenerContainerFactory<String, Object> kafkaListenerContainerFactory(
            ObjectMapper kafkaObjectMapper) {

        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "custom-kafka-default-group");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        JsonDeserializer<Object> jsonDeserializer = new JsonDeserializer<>(kafkaObjectMapper);
        jsonDeserializer.addTrustedPackages("*");
        // Honor type headers so EventWrapper<?> sent on main topic can be deserialized
        jsonDeserializer.setUseTypeHeaders(true);

        ConsumerFactory<String, Object> consumerFactory = new DefaultKafkaConsumerFactory<>(
                props,
                new StringDeserializer(),
                jsonDeserializer
        );

        ConcurrentKafkaListenerContainerFactory<String, Object> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);

        // Disable container-level error handling - let aspect handle retries
        factory.setCommonErrorHandler(null);

        // Enable manual ack mode for proper message acknowledgment
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);

        return factory;
    }


    /*
        USE SAME CONFIG AS DLQ CONSUMER FACTORY BELOW
     */
//    @Bean
//    @ConditionalOnMissingBean(name = "dlqReaderFactory")
//    public ConsumerFactory<String, EventWrapper<?>> dlqReaderFactory(ObjectMapper kafkaObjectMapper) {
//        Map<String, Object> props = new HashMap<>();
//        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
//        props.put(ConsumerConfig.GROUP_ID_CONFIG, "custom-kafka-dlq-group");
//        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
//
//        JsonDeserializer<EventWrapper<?>> deserializer =
//                new JsonDeserializer<>(EventWrapper.class, kafkaObjectMapper);
//        deserializer.addTrustedPackages("*");
//        deserializer.setUseTypeHeaders(false);
//
//        return new DefaultKafkaConsumerFactory<>(
//                props,
//                new StringDeserializer(),
//                deserializer
//        );
//    }

    /**
     * Consumer factory for DLQ messages (EventWrapper).
     * Automatically handles deserialization of wrapped events.
     */
    @Bean
    @ConditionalOnMissingBean(name = "dlqConsumerFactory")
    public ConsumerFactory<String, EventWrapper<?>> dlqConsumerFactory(ObjectMapper kafkaObjectMapper) {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "custom-kafka-dlq-group");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        JsonDeserializer<EventWrapper<?>> deserializer =
                new JsonDeserializer<>(EventWrapper.class, kafkaObjectMapper);
        deserializer.addTrustedPackages("*");
        deserializer.setUseTypeHeaders(false);

        return new DefaultKafkaConsumerFactory<>(
                props,
                new StringDeserializer(),
                deserializer
        );
    }

    /**
     * Container factory for DLQ listeners.
     * Use this factory in your @KafkaListener for DLQ topics:
     * @KafkaListener(topics = "my-dlq", containerFactory = "dlqKafkaListenerContainerFactory")
     */
    @Bean
    @ConditionalOnMissingBean(name = "dlqKafkaListenerContainerFactory")
    public ConcurrentKafkaListenerContainerFactory<String, EventWrapper<?>> dlqKafkaListenerContainerFactory(
            ConsumerFactory<String, EventWrapper<?>> dlqConsumerFactory) {
        ConcurrentKafkaListenerContainerFactory<String, EventWrapper<?>> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(dlqConsumerFactory);
        return factory;
    }
}