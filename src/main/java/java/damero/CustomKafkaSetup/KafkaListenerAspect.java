package java.damero.CustomKafkaSetup;

import lombok.extern.slf4j.Slf4j;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.springframework.stereotype.Component;

import java.damero.annotations.CustomKafkaListener;

@Aspect
@Component
public class KafkaListenerAspect {

    //ProceedingJoinPoint gives u full access of method and execution
    @Around("@annotation(customKafkaListener)")
    public Object kafkaListener(ProceedingJoinPoint pjp, CustomKafkaListener customKafkaListener) throws Throwable {

        int attempts = 0;

        Throwable lastException = null;

        while(attempts < customKafkaListener.maxAttempts()){

            try {
                return pjp.proceed();
            } catch (Throwable e) {
                lastException = e;
                attempts++;


            }
        }
        throw lastException;

    }
}
