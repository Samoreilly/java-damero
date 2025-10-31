package net.damero.Kafka.DeadLetterQueueAPI;

import net.damero.Kafka.CustomObject.EventWrapper;
import net.damero.Kafka.DeadLetterQueueAPI.ReadFromDLQ.ReadFromDLQConsumer;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import java.util.List;

@RestController
public class DLQController {

    private final ReadFromDLQConsumer readFromDLQConsumer;

    public DLQController(ReadFromDLQConsumer readFromDLQConsumer) {
        this.readFromDLQConsumer = readFromDLQConsumer;
    }

    @GetMapping("/dlq")
    public List<EventWrapper<?>> getDLQ(){

        List<EventWrapper<?>> dlqEvents = readFromDLQConsumer.readFromDLQ("test-dlq");

        if(dlqEvents.isEmpty() || dlqEvents == null){
            EventWrapper<String> emptyWrapper = new EventWrapper<>();
            emptyWrapper.setEvent("Nothing in Dead Letter Queue");
            return List.of(emptyWrapper);
        }
        return dlqEvents;
    }
}
