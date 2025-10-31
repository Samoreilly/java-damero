package net.damero;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.EnableAspectJAutoProxy;



@SpringBootApplication
@EnableAspectJAutoProxy
@ComponentScan(basePackages = {
        "net.damero",
        "net.damero.Kafka.CustomKafkaSetup",
        "net.damero.Kafka.CustomObject",
        "net.damero.Kafka.KafkaServices",
        "net.damero.Kafka.Annotations",
        "net.damero.Kafka.ManageListener"
})
public class TestApplication {
    public static void main(String[] args) {
        SpringApplication.run(TestApplication.class, args);
    }
}