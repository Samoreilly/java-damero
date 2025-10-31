package net.damero.Kafka.ManageListener;

import jakarta.annotation.PostConstruct;
import org.reflections.Reflections;
import org.reflections.scanners.Scanners;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Service;
import org.springframework.kafka.annotation.KafkaListener;

import net.damero.Kafka.CustomKafkaSetup.CustomKafkaListenerConfig;
import net.damero.Kafka.Annotations.CustomKafkaListener;
import net.damero.Kafka.Annotations.MessageListener;
import java.lang.reflect.Method;
import java.util.Set;

//https://www.baeldung.com/reflections-library


@Service
public class ListenerSetup {

    @Autowired
    ApplicationContext context;

    @PostConstruct
    public void listenerSetup(){

        Reflections reflections = new Reflections("java.damero", Scanners.TypesAnnotated);

        Set<Method> methods = reflections.getMethodsAnnotatedWith(CustomKafkaListener.class);
        for (Method method : methods) {

            if (!method.isAnnotationPresent(KafkaListener.class)) {
                throw new IllegalStateException("@CustomKafkaListener on " + method.getName() + " must also have @KafkaListener annotation"
                );
            }

            //This gets all methods annotated with @CustomKafkaListener
            CustomKafkaListener customKafkaListener = method.getAnnotation(CustomKafkaListener.class);
            KafkaListener kafkaListener = method.getAnnotation(KafkaListener.class);

            //Verifies the method is annotated with @CustomKafkaListener and @KafkaListener
            if(kafkaListener.topics().length > 0 && !customKafkaListener.topic().equals(kafkaListener.topics()[0])){
                throw new IllegalStateException("@CustomKafkaListener topic must match @KafkaListener topic");
            }

        }
    }
}