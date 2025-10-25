package java.damero;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;


@SpringBootApplication
public class Application{


    //PLAN AND DESIGN

    //KAFKA
    //Create MesssageListener annotation that will be used to annotate a class for dlq
    //the class must also have @KafkaListener

	public static void main(String[] args) {
		SpringApplication.run(Application.class, args);
	}

}
