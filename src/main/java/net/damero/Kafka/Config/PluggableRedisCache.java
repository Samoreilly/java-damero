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
            try {
                logger.debug("Storing in Redis - key: {}, value type: {}", cacheKeyPrefix + key, value.getClass().getSimpleName());
                redisTemplate.opsForValue().set(cacheKeyPrefix + key, value);
            } catch (Exception e) {
                logger.error("Failed to store key '{}' in Redis: {}. Cache operation skipped.",
                    key, e.getMessage());
                // Gracefully degrade - don't cache if Redis is down
                // This prevents application crashes but may lead to duplicate processing
            }
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
            try {
                redisTemplate.opsForValue().set(cacheKeyPrefix + key, value, ttl);
            } catch (Exception e) {
                logger.error("Failed to store key '{}' in Redis with TTL: {}. Cache operation skipped.",
                    key, e.getMessage());
                // Gracefully degrade - don't cache if Redis is down
            }
        } else if (caffeineCache != null && value instanceof Integer) {
            // Caffeine cache has TTL configured at construction time, so we just put
            caffeineCache.put(key, (Integer) value);
        }
    }

    public boolean contains(String key) {
        if (redisTemplate != null) {
            try {
                return Boolean.TRUE.equals(redisTemplate.hasKey(cacheKeyPrefix + key));
            } catch (Exception e) {
                logger.error("Failed to check key '{}' in Redis: {}. Returning false.",
                    key, e.getMessage());
                // Return false to indicate key not found when Redis is unavailable
                // This may cause duplicate processing but prevents crashes
                return false;
            }
        } else if (caffeineCache != null) {
            return caffeineCache.get(key) != null;
        }
        return false;
    }

    public Object get(String key) {
        if (redisTemplate != null) {
            try {
                return redisTemplate.opsForValue().get(cacheKeyPrefix + key);
            } catch (Exception e) {
                logger.error("Failed to retrieve key '{}' from Redis: {}. Returning null.",
                    key, e.getMessage());
                // Return null when Redis is unavailable
                // Caller will handle missing cache entry appropriately
                return null;
            }
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
            try {
                redisTemplate.delete(cacheKeyPrefix + key);
            } catch (Exception e) {
                logger.error("Failed to remove key '{}' from Redis: {}. Remove operation skipped.",
                    key, e.getMessage());
                // Gracefully degrade - don't fail if Redis is down
            }
        } else if (caffeineCache != null) {
            caffeineCache.remove(key);
        }
    }
}
