package net.damero.CustomKafkaSetup;

import net.damero.CustomObject.EventMetadata;
import net.damero.CustomObject.EventWrapper;
import net.damero.KafkaServices.KafkaDLQ;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.reflect.MethodSignature;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import net.damero.Annotations.CustomKafkaListener;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

import static net.damero.CustomKafkaSetup.DelayMethod.EXPO;
import static net.damero.CustomKafkaSetup.DelayMethod.LINEAR;

@Aspect
@Component
public class KafkaListenerAspect {

    @Autowired
    private ApplicationContext context;

    @Autowired
    private ConcurrentKafkaListenerContainerFactory<String, Object> defaultFactory;

    @Autowired
    private KafkaTemplate<?, ?> defaultKafkaTemplate;

    @Around("@annotation(customKafkaListener)")
    public Object kafkaListener(ProceedingJoinPoint pjp, CustomKafkaListener customKafkaListener) throws Throwable {
        System.out.println("ASPECT TRIGGERED for topic: " + customKafkaListener.topic());
        KafkaTemplate<?, ?> kafkaTemplate = defaultKafkaTemplate;

        // Check for custom kafka template
        Class<?> factoryClass = customKafkaListener.kafkaTemplate();
        if (!factoryClass.equals(void.class)) {
            kafkaTemplate = (KafkaTemplate<?, ?>) context.getBean(factoryClass);
        }

        // Build config - removed consumerFactory and eventType
        CustomKafkaListenerConfig config = CustomKafkaListenerConfig.builder()
                .topic(customKafkaListener.topic())
                .dlqTopic(customKafkaListener.dlqTopic())
                .maxAttempts(customKafkaListener.maxAttempts())
                .delay(customKafkaListener.delay())
                .delayMethod(customKafkaListener.delayMethod())
                .kafkaTemplate(kafkaTemplate)
                // Remove these two lines if they exist:
                // .eventType(eventType)
                // .consumerFactory(effectiveConsumerFactory)
                .build();

        Object event = pjp.getArgs().length > 0 ? pjp.getArgs()[0] : null;

        Acknowledgment acknowledgment = null;//to stop reprocessing the same event after its being passed to DLQ
        for (Object arg : pjp.getArgs()) {
            if (arg instanceof Acknowledgment) {
                acknowledgment = (Acknowledgment) arg;
                break;
            }
        }
        int attempts = 0;
        Throwable lastException = null;
        LocalDateTime firstFailureDateTime = null;
        LocalDateTime lastFailureDateTime = null;

        while(attempts < customKafkaListener.maxAttempts()){
            attempts++;

            try {

                Object result = pjp.proceed();

                if (acknowledgment != null) {
                    acknowledgment.acknowledge();
                }

                return result;

            } catch (Throwable e) {
                if (e instanceof ClassCastException || e instanceof IllegalArgumentException) {
                    if (acknowledgment != null) {
                        acknowledgment.acknowledge();//prevent reprocessing of the same event
                    }
                    throw e;
                }

                if(firstFailureDateTime == null){
                    firstFailureDateTime = LocalDateTime.now();
                }

                boolean sendToDLQ = attempts >= customKafkaListener.maxAttempts();

                if(event != null){
                    lastFailureDateTime = LocalDateTime.now();
                    EventMetadata eventMetadata = new EventMetadata(firstFailureDateTime, lastFailureDateTime, attempts);

                    // Wrap the raw event before sending to DLQ
                    EventWrapper<Object> wrappedEvent = new EventWrapper<>(event, LocalDateTime.now(), eventMetadata);

                    KafkaDLQ.sendToDLQ(config.getKafkaTemplate(), config.getDlqTopic(), wrappedEvent, e, sendToDLQ, eventMetadata);
                }
                lastException = e;

                if(sendToDLQ){
                    if (acknowledgment != null) {
                        System.out.println("⚠️ Acknowledging message after max attempts reached");
                        acknowledgment.acknowledge();
                    }
                    throw e;
                }

                DelayMethod delayMethod = customKafkaListener.delayMethod();
                double baseDelay = customKafkaListener.delay();
                long sleepTime;

                switch (delayMethod) {
                    case LINEAR -> sleepTime = (long) (baseDelay * attempts);
                    case EXPO -> sleepTime = (long) (baseDelay * Math.min(DelayMethod.MAX.amount, Math.pow(2, attempts)));
                    case MAX -> sleepTime = DelayMethod.MAX.amount;
                    default -> sleepTime = (long) baseDelay;
                }

                Thread.sleep(sleepTime);
            }
        }

        throw lastException;
    }



}
