package net.damero.CustomKafkaSetup.RegisterConfig;

import org.springframework.stereotype.Service;

import net.damero.CustomKafkaSetup.CustomKafkaListenerConfig;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

@Service
public class RegisterConfig {

    //EACH LISTENER METHOD SHOULD HAVE THERE OWN CONFIG
    private final Map<Method, CustomKafkaListenerConfig> methodConfig = new HashMap<>();

    public void registerConfig(Method method, CustomKafkaListenerConfig config) {
        methodConfig.put(method, config);
    }
    public CustomKafkaListenerConfig getConfig(Method method) {
        return methodConfig.get(method);
    }

}
