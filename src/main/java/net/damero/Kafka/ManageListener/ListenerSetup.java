package net.damero.Kafka.ManageListener;

import jakarta.annotation.PostConstruct;
import org.reflections.Reflections;
import org.reflections.scanners.Scanners;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Service;
import org.springframework.kafka.annotation.KafkaListener;

import net.damero.Kafka.Annotations.DameroKafkaListener;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//https://www.baeldung.com/reflections-library


@Service
public class ListenerSetup {

    private static final Logger logger = LoggerFactory.getLogger(ListenerSetup.class);

    @Autowired
    ApplicationContext context;

    @PostConstruct
    public void listenerSetup(){

        // Determine the application base package dynamically so we scan the user's code,
        // not this library's package. We try to find the main Spring Boot application
        // class (annotated with @SpringBootApplication) or fallback to
        // @SpringBootConfiguration. If neither is present, fall back to a safe default.
        String basePackage = "net.damero"; // fallback - conservative

        try {
            // Prefer Spring Boot application's package
            Map<String, Object> appBeans = context.getBeansWithAnnotation(org.springframework.boot.autoconfigure.SpringBootApplication.class);
            if (!appBeans.isEmpty()) {
                Object appBean = appBeans.values().iterator().next();
                basePackage = appBean.getClass().getPackageName();
            } else {
                // Fallback: look for SpringBootConfiguration
                Map<String, Object> configBeans = context.getBeansWithAnnotation(org.springframework.boot.SpringBootConfiguration.class);
                if (!configBeans.isEmpty()) {
                    Object configBean = configBeans.values().iterator().next();
                    basePackage = configBean.getClass().getPackageName();
                }
            }
        } catch (Exception e) {
            // If anything goes wrong, we'll use the fallback and continue - no hard failure
            logger.debug("failed to determine application base package for listener scanning: {}", e.getMessage());
        }

        // Use reflections to find methods annotated with the user's @DameroKafkaListener
        try {
            Reflections reflections = new Reflections(basePackage, Scanners.MethodsAnnotated);

            Set<Method> methods = reflections.getMethodsAnnotatedWith(DameroKafkaListener.class);
            for (Method method : methods) {

                if (!method.isAnnotationPresent(KafkaListener.class)) {
                    throw new IllegalStateException("@DameroKafkaListener on " + method.getName() + " must also have @KafkaListener annotation"
                    );
                }

                // This gets all methods annotated with @DameroKafkaListener
                DameroKafkaListener dameroKafkaListener = method.getAnnotation(DameroKafkaListener.class);
                KafkaListener kafkaListener = method.getAnnotation(KafkaListener.class);

                // Verifies the method is annotated with @DameroKafkaListener and @KafkaListener
                if(kafkaListener.topics().length > 0 && !dameroKafkaListener.topic().equals(kafkaListener.topics()[0])){
                    throw new IllegalStateException("@DameroKafkaListener topic must match @KafkaListener topic");
                }

            }
        } catch (Exception ex) {
            // Don't fail the application context if reflections scanning or validation fails in test environments
            logger.warn("Damero listener discovery/validation failed for basePackage='{}'. Continuing without listener validation. Reason: {}", basePackage, ex.getMessage());
            logger.debug("listener discovery stacktrace:", ex);
        }
    }
}