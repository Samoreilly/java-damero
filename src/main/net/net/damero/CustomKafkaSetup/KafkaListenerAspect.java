package net.damero.CustomKafkaSetup;

import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import net.damero.annotations.CustomKafkaListener;

@Aspect
@Component
public class KafkaListenerAspect {

    @Autowired
    public KafkaTemplate<String, Object> kafkaTemplate;

    //ProceedingJoinPoint gives u full access of method and execution
    @Around("@annotation(customKafkaListener)")
    public Object kafkaListener(ProceedingJoinPoint pjp, CustomKafkaListener customKafkaListener) throws Throwable {

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

}
