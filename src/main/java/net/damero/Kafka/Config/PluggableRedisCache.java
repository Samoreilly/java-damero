package net.damero.Kafka.Config;

import net.damero.Kafka.Aspect.Components.CaffeineCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.RedisTemplate;

import java.time.Duration;

public class PluggableRedisCache {

    private static final Logger logger = LoggerFactory.getLogger(PluggableRedisCache.class);

    private final RedisTemplate<String, Object> redisTemplate;
    private final CaffeineCache caffeineCache;
    private final String cacheKeyPrefix = "internal_cache:";

    // Redis constructor
    public PluggableRedisCache(RedisTemplate<String, Object> redisTemplate) {
        this.redisTemplate = redisTemplate;
        if (redisTemplate != null) {
            logger.info("=== PluggableRedisCache initialized with Redis backend ===");
        }
        this.caffeineCache = null;
    }

    // overloaded constructor for Caffeine in memory cache
    public PluggableRedisCache(CaffeineCache caffeineCache) {
        this.redisTemplate = null;
        this.caffeineCache = caffeineCache;
        logger.info("=== PluggableRedisCache initialized with Caffeine backend ===");
    }

    public void put(String key, Object value) {
        if (redisTemplate != null) {
            logger.debug("Storing in Redis - key: {}, value type: {}", cacheKeyPrefix + key, value.getClass().getSimpleName());
            redisTemplate.opsForValue().set(cacheKeyPrefix + key, value);
        } else if (caffeineCache != null && value instanceof Integer) {
            caffeineCache.put(key, (Integer) value);
        }
    }

    /**
     * Put a value in the cache with a time-to-live (TTL).
     * After the TTL expires, the entry will be automatically removed.
     *
     * @param key the cache key
     * @param value the value to cache
     * @param ttl the time-to-live duration
     */
    public void put(String key, Object value, Duration ttl) {
        if (redisTemplate != null) {
            redisTemplate.opsForValue().set(cacheKeyPrefix + key, value, ttl);
        } else if (caffeineCache != null && value instanceof Integer) {
            // Caffeine cache has TTL configured at construction time, so we just put
            caffeineCache.put(key, (Integer) value);
        }
    }

    public boolean contains(String key) {
        if (redisTemplate != null) {
            return Boolean.TRUE.equals(redisTemplate.hasKey(cacheKeyPrefix + key));
        } else if (caffeineCache != null) {
            return caffeineCache.get(key) != null;
        }
        return false;
    }

    public Object get(String key) {
        if (redisTemplate != null) {
            return redisTemplate.opsForValue().get(cacheKeyPrefix + key);
        } else if (caffeineCache != null) {
            return caffeineCache.get(key);
        }
        return null;
    }

    public Integer getOrDefault(String key, Integer defaultValue) {
        Object value = get(key);
        if (value instanceof Integer) {
            return (Integer) value;
        }
        return defaultValue;
    }

    public void remove(String key) {
        if (redisTemplate != null) {
            redisTemplate.delete(cacheKeyPrefix + key);
        } else if (caffeineCache != null) {
            caffeineCache.remove(key);
        }
    }
}
