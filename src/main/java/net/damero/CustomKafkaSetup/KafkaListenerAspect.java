package net.damero.CustomKafkaSetup;

import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import net.damero.annotations.CustomKafkaListener;

@Aspect
@Component
public class KafkaListenerAspect {

    @Autowired
    private ApplicationContext context;

    @Autowired
    private ConcurrentKafkaListenerContainerFactory<String, Object> defaultFactory;

    @Autowired
    private KafkaTemplate<? super String, ? super Object> kafkaTemplate;

    //ProceedingJoinPoint gives u full access of method and execution
    @Around("@annotation(customKafkaListener)")
    public Object kafkaListener(ProceedingJoinPoint pjp, CustomKafkaListener customKafkaListener) throws Throwable {

        //defaults to default consumer factory
        ConsumerFactory<?, ?> customConsumerFactory = defaultFactory.getConsumerFactory();

        //check if user provided custom factory
        if(!customKafkaListener.consumerFactory().equals(void.class)){
            Class<?> factoryClass = customKafkaListener.consumerFactory();
            customConsumerFactory = (ConsumerFactory<?, ?>) context.getBean(factoryClass);//backoff to default factory if no custom factory is provided
        }


        CustomKafkaListenerConfig.Builder builder = new CustomKafkaListenerConfig.Builder()
                .topic(customKafkaListener.topic())
                .dlqTopic(customKafkaListener.dlqTopic())
                .maxAttempts(customKafkaListener.maxAttempts())
                .delay(customKafkaListener.delay())
                .delayMethod(customKafkaListener.delayMethod())
                .kafkaTemplate(kafkaTemplate)
                .consumerFactory(customConsumerFactory);

        //interface defaults kafkatemplte to void.class
        //if the kafkatemplate is provided we will use getBean to get that template
        //otherwise defaulit to the default
        if (!customKafkaListener.kafkaTemplate().equals(void.class)) {
            Class<?> kafkaClass = customKafkaListener.kafkaTemplate();
            KafkaTemplate<?, ?> customTemplate = (KafkaTemplate<?, ?>) context.getBean(kafkaClass);
            builder.kafkaTemplate(customTemplate);
        } else {
            builder.kafkaTemplate(kafkaTemplate);
        }

        Object event = pjp.getArgs().length > 0 ? pjp.getArgs()[0] : null;//gets event from queue

        int MAX = DelayMethod.MAX.amount;
        int attempts = 0;
        Throwable lastException = null;

        while(attempts < customKafkaListener.maxAttempts()){

            try {
                return pjp.proceed();
            } catch (Throwable e) {

                attempts++;

                if(attempts >= customKafkaListener.maxAttempts() && event != null){

                    kafkaTemplate.send(customKafkaListener.dlqTopic(), event);

                    throw e;
                }

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


    //get factory from config if it exists, else use default factory
//    @Bean
//    public ConcurrentKafkaListenerContainerFactory<String, Object> getFactory(CustomKafkaListenerConfig config) {
//        if (config.getConsumerFactory() != null) {
//            ConcurrentKafkaListenerContainerFactory<String, Object> factory =
//                    new ConcurrentKafkaListenerContainerFactory<>();
//            factory.setConsumerFactory(config.getConsumerFactory());
//            factory.setConcurrency(3);
//            factory.getContainerProperties().setAckMode(defaultFactory.getContainerProperties().getAckMode());
//            return factory;
//        }
//        return defaultFactory;
//    }

}
