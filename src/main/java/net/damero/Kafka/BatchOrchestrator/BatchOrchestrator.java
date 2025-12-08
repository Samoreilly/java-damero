package net.damero.Kafka.BatchOrchestrator;

import net.damero.Kafka.Annotations.CustomKafkaListener;
import net.damero.Kafka.Config.PluggableRedisCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

@Component
public class BatchOrchestrator {

    Logger logger = LoggerFactory.getLogger(BatchOrchestrator.class);

    private final PluggableRedisCache cache;

    ConcurrentHashMap<String, AtomicLong> batchCounters = new ConcurrentHashMap<>();

    public BatchOrchestrator(PluggableRedisCache cache) {
        this.cache = cache;
    }

    public void orchestrate(CustomKafkaListener customKafkaListener, String topic){
        if(limitReached(customKafkaListener, topic)){
            logger.info("Batch limit reached for topic: " + topic);
            return;
        }


        Long batchValue = incrementBatchCounter(topic);

        logger.info("Processing batch for topic: " + topic + ", current count: " + batchValue);

    }

    public long incrementBatchCounter(String topic){
        return batchCounters.computeIfAbsent(topic, k -> new AtomicLong(0)).incrementAndGet();
    }

    public boolean limitReached(CustomKafkaListener customKafkaListener, String topic){
        int batchSize = customKafkaListener.batchSize();
        AtomicLong batchCounter = batchCounters.get(topic);

        //if it doesn't exist, exit method
        //it will be made where incrementBatchCounter is called in orchestrate()
        if (batchCounter == null) {
            return false;
        }
        return batchCounter.intValue() >= batchSize;

    }

}
