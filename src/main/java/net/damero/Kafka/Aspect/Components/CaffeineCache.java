package net.damero.Kafka.Aspect.Components;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

public class CaffeineCache {

    private final Cache<String, Integer> cache;

    public CaffeineCache() {
        this.cache = Caffeine.newBuilder()
            .maximumSize(1000)
            .build();
    }
    
    public void put(String key, Integer value) {
        cache.put(key, value);
    }

    public Integer get(String key) {
        return cache.getIfPresent(key);
    }

    public Integer getOrDefault(String key, Integer defaultValue) {
        Integer value = cache.getIfPresent(key);
        return value != null ? value : defaultValue;
    }

    public void remove(String key) {
        cache.invalidate(key);
    }
}