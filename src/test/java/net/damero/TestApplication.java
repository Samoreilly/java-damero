package net.damero;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.EnableAspectJAutoProxy;



@SpringBootApplication
@EnableAspectJAutoProxy
@ComponentScan(basePackages = {
        "net.damero",
        "net.damero.Kafka.Config",
        "net.damero.Kafka.Aspect",
        "net.damero.Kafka.CustomObject",
        "net.damero.Kafka.KafkaServices",
        "net.damero.Kafka.Annotations",
        "net.damero.Kafka.ManageListener",
        "net.damero.Kafka.Resilience",
        "net.damero.Kafka.Factory"
})
public class TestApplication {
    public static void main(String[] args) {
        SpringApplication.run(TestApplication.class, args);
    }
}