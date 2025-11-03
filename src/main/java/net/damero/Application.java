package net.damero;

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

// cat << 'EOF'

// YES - It can be made MUCH more user friendly

// Current user code (boilerplate every listener):
// ----------------------------------------------
// Object value = record.value();
// if (value instanceof OrderEvent oe) {
//     order = oe;
// } else if (value instanceof EventWrapper<?> wrapper) {
//     order = (OrderEvent) wrapper.getEvent();
// }

// Simplified user code (after fix):
// ---------------------------------
// OrderEvent order = (OrderEvent) record.value();

// Difficulty to implement: MODERATE (6/10)
// - Need to modify ConsumerRecord before pjp.proceed()
// - Create wrapper ConsumerRecord with unwrapped value
// - Pass new args array to proceed()

// Impact: HIGH
// - Removes all boilerplate
// - Library handles unwrapping transparently
// - Users write cleaner code

// Worth doing? YES

// EOF

