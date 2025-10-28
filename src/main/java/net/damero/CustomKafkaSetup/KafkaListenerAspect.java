package net.damero.CustomKafkaSetup;

import net.damero.CustomObject.EventMetadata;
import net.damero.KafkaServices.KafkaDLQ;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import net.damero.Annotations.CustomKafkaListener;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

@Aspect
@Component
public class KafkaListenerAspect {

    @Autowired
    private ApplicationContext context;

    @Autowired
    private ConcurrentKafkaListenerContainerFactory<String, Object> defaultFactory;

    @Autowired
    private KafkaTemplate<?, ?> defaultKafkaTemplate;

    /**
     * CURRENT IMPLEMENTATION:
     * Retries block the consumer thread.
     * Ensure maxAttempts * delay < max.poll.interval.ms
     * For long delays, consider using a retry topic pattern instead.
     */
    //ProceedingJoinPoint gives u full access of method and execution
    @Around("@annotation(customKafkaListener)")
    public Object kafkaListener(ProceedingJoinPoint pjp, CustomKafkaListener customKafkaListener) throws Throwable {

        //defaults to default consumer factory

        KafkaTemplate<?, ?> kafkaTemplate = defaultKafkaTemplate;

        //set default consumer for listener
        ConcurrentKafkaListenerContainerFactory<String, Object> factory = defaultFactory;

        ConsumerFactory<? super String, ? super Object> customConsumerFactory;

        //check if user provided custom factory
        if (!customKafkaListener.consumerFactory().equals(void.class)) {

            Class<?> factoryClass = customKafkaListener.consumerFactory();
            customConsumerFactory = (ConsumerFactory<? super String, ? super Object>) context.getBean(factoryClass);
        }else{

            //defaults to default consumer factory
            customConsumerFactory = defaultFactory.getConsumerFactory();
        }

        factory.setConsumerFactory(customConsumerFactory);

        //checks for custom kafka template
        Class<?> factoryClass = customKafkaListener.kafkaTemplate();
        if (!factoryClass.equals(void.class)) {
            kafkaTemplate = (KafkaTemplate<?, ?>) context.getBean(factoryClass);//gets the custom kafka template
        }

        //builds CustomKafkaListenerConfig
        CustomKafkaListenerConfig.Builder builder = CustomKafkaListenerConfig.builder()
                .topic(customKafkaListener.topic())
                .dlqTopic(customKafkaListener.dlqTopic())
                .maxAttempts(customKafkaListener.maxAttempts())
                .delay(customKafkaListener.delay())
                .delayMethod(customKafkaListener.delayMethod())
                .kafkaTemplate(kafkaTemplate)
                .consumerFactory(customConsumerFactory);

        CustomKafkaListenerConfig config = builder.build();

        //interface defaults kafkatemplte to void.class
        //if the kafkatemplate is provided we will use getBean to get that template
        //otherwise defaulit to the default

        Object event = pjp.getArgs().length > 0 ? pjp.getArgs()[0] : null;//gets event from queue

        int MAX = DelayMethod.MAX.amount;
        int attempts = 0;
        Throwable lastException = null;
        LocalDateTime firstFailureDateTime = null;
        LocalDateTime lastFailureDateTime = null;


        while(attempts < customKafkaListener.maxAttempts()){

            attempts++;

            try {
                return pjp.proceed();//continues original method, in this case it would be the listener method that the user
                                     //has annotated with customKafkaListener
            } catch (Throwable e) {

                if(firstFailureDateTime == null){
                    firstFailureDateTime = LocalDateTime.now();
                }

                //boolean method to make sure we log EVERY exception and not just when we exceed max attempts
                boolean sendToDLQ = attempts >= customKafkaListener.maxAttempts();

                if(event != null){
                    lastFailureDateTime = LocalDateTime.now();
                    EventMetadata eventMetadata = new EventMetadata(firstFailureDateTime, lastFailureDateTime, attempts);
                    KafkaDLQ.sendToDLQ(config.getKafkaTemplate(), config.getDlqTopic(), event, e, sendToDLQ, eventMetadata);
                }

                // If we've exceeded max attempts, throw the exception
                if(sendToDLQ){
                    throw e;
                }

                //otherwise, retry
                DelayMethod delayMethod = customKafkaListener.delayMethod();
                double baseDelay = customKafkaListener.delay();

                switch (delayMethod){
                    case LINEAR ->  Thread.sleep((long)(baseDelay * attempts));
                    case EXPO -> Thread.sleep((long)(baseDelay * Math.min(MAX, Math.pow(2, attempts))));
                    case MAX -> Thread.sleep((long)(MAX));
                    default -> Thread.sleep((long)(baseDelay));
                }

                lastException = e;
            }
        }

        throw lastException;
    }

}
