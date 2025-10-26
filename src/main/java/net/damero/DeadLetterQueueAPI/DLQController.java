package net.damero.DeadLetterQueueAPI;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class DLQController {

    @GetMapping("/dlq")
    public String getDLQ(){
        return "Dead Letter Queue";
    }
}
