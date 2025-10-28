package net.damero.KafkaServices;

import net.damero.CustomObject.EventMetadata;
import net.damero.CustomObject.EventWrapper;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.ArrayList;
import java.util.List;

import static net.damero.CustomObject.GlobalExceptionMapLogger.exceptions;

public class KafkaDLQ {

    //static method to call it avoid uneccessary injections
    @SuppressWarnings({"unchecked", "rawtypes"})
    public static void sendToDLQ(KafkaTemplate<?, ?> kafkaTemplate, String topic, EventWrapper eventWrapper, Throwable throwable, boolean sendToDLQ, EventMetadata eventMetadata){


        exceptions.computeIfAbsent(eventWrapper, k -> new ArrayList<>()).add(throwable);

        //only send to DLQ if we exceed max attempts
        if(sendToDLQ){
            ((KafkaTemplate) kafkaTemplate).send(topic, eventWrapper);
        }
    }
}
