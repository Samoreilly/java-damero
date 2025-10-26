package net.damero.ManageListener;

import jakarta.annotation.PostConstruct;
import org.reflections.Reflections;
import org.reflections.scanners.Scanners;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Service;
import org.springframework.kafka.annotation.KafkaListener;

import net.damero.CustomKafkaSetup.CustomKafkaListenerConfig;
import net.damero.Annotations.CustomKafkaListener;
import net.damero.Annotations.MessageListener;
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

//EXAMPLE OF A CLASS THAT USES @KafkaListener

//    @KafkaListener(topics = "transactions", groupId = "in-transactions", containerFactory = "factory")
//    public void transactionPipeline(@Payload TransactionRequest userData) throws Exception {
//
//        //handleNeuralTransaction.handleTransaction(userData);
//
//        try {
//
//
//            anomalyTraining.anomalyPipeline(userData);
//
//            notificationService.sendNotification(userData, "Processing your transaction");
//            System.out.println(userData);
//
//
//            long currentEpoch = userData.getTime().toEpochSecond(ZoneOffset.UTC);
//            //normalize time to fit into the models time range. As the models time range is around 2024 and input data is in 2025
//            double epochSeconds = 1719650000 + (currentEpoch % 60000);
//            service.trainModel();
//            boolean isFraud = service.predictFraud(Double.parseDouble(userData.getData()), epochSeconds, userData.getLatitude(), userData.getLongitude()); // amount, lat, lng
//            double fraudProb = service.getFraudProbability(Double.parseDouble(userData.getData()), epochSeconds, userData.getLatitude(), userData.getLongitude());
//
//
//            System.out.printf("Fraud Prediction: %s\n", isFraud ? "FRAUD" : "LEGITIMATE");
//            System.out.printf("Fraud Probability: %.2f%% (%.4f)\n", fraudProb * 100, fraudProb);
//            if (transactionSecurityCheck.checkFraud(fraudProb, isFraud, userData)) {
//                System.out.println("Fraud detected - exiting early from pipeline");
//                return;
//            }
//            // retrieve users transactions as a list
//            List<TransactionRequest> transactions = getTransactions(userData);
//            boolean result = pipeline.process(userData, transactions);
//            System.out.println(result + "-----------------------HANDLER RESULT");
//            if (result) {
//                saveTransaction(userData, isFraud);//save transaction
//            } else {
//                notificationService.sendNotification(userData, "Transaction pipeline error");
//            }
//            System.out.println("Cached");
/// /            if(!test){
/// /                test = true;
/// /                System.out.println("RETRYING" + userData.toString());
/// /                throw new RuntimeException("TESTING---0--000-0-0A-0DA-D0AD-A0DA-D0A-0");
/// /            }
//            System.out.println("RETRYING" + userData.toString());
//
//        }catch(Exception e){
//
//            //Dead letter queue
//            DatabaseDTO deadLetterObject = new DatabaseDTO();
//            deadLetterObject.setId(userData.getId());
//            deadLetterObject.setData(userData.getData());
//            deadLetterObject.setTime(userData.getTime());
//            deadLetterObject.setClientIp(userData.getClientIp());
//            deadLetterObject.setResult(userData.getResult());
//            deadLetterObject.setLatitude(userData.getLatitude());
//            deadLetterObject.setLongitude(userData.getLongitude());
//            deadLetterObject.setIsFraud(userData.getIsFraud());
//
//            viewDeadLetterQueue.sendToQueue(deadLetterObject);
//            System.out.println("dlq error");
//            List<DatabaseDTO> dlq = viewDeadLetterQueue.getDLQEvents();
//            System.out.println("Printing");
//
//            for(DatabaseDTO dl : dlq){
//                System.out.println(dl);
//            }
//            throw e;
//        }
//
//    }
//    @RetryableTopic(
//            attempts = "3",
//            dltStrategy = DltStrategy.ALWAYS_RETRY_ON_ERROR
//    )
//    @KafkaListener(topics = "transactions-retry-0",  groupId = "in-transactions-retry", containerFactory = "factory")
//    public void retry(@Payload TransactionRequest userData, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic){
//        System.out.println("----------------------------------------------------------RETRYING----------------------------------------------------------");
//        log.info("Event on main topic={}, payload={}", topic, userData);
//    }
//
//}
