package net.damero.Kafka.Config;

import net.damero.Kafka.Aspect.Components.CaffeineCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.lang.Nullable;

import java.time.Duration;

public class PluggableRedisCache {

    private static final Logger logger = LoggerFactory.getLogger(PluggableRedisCache.class);

    private final RedisTemplate<String, Object> redisTemplate;
    private final CaffeineCache caffeineCache;
    private final RedisHealthCheck healthCheck;
    private final String cacheKeyPrefix = "internal_cache:";

    // Constructor with both Redis and Caffeine backends plus health check (for automatic failover)
    public PluggableRedisCache(RedisTemplate<String, Object> redisTemplate,
                              CaffeineCache caffeineCache,
                              @Nullable RedisHealthCheck healthCheck) {
        this.redisTemplate = redisTemplate;
        this.caffeineCache = caffeineCache;
        this.healthCheck = healthCheck;
        if (redisTemplate != null && caffeineCache != null) {
            logger.info("=== PluggableRedisCache initialized with Redis + Caffeine failover ===");
        }
    }

    // Redis constructor with health check
    public PluggableRedisCache(RedisTemplate<String, Object> redisTemplate, @Nullable RedisHealthCheck healthCheck) {
        this.redisTemplate = redisTemplate;
        this.caffeineCache = null;
        this.healthCheck = healthCheck;
        if (redisTemplate != null) {
            logger.info("=== PluggableRedisCache initialized with Redis backend ===");
        }
    }

    // Redis constructor without health check (backwards compatibility)
    public PluggableRedisCache(RedisTemplate<String, Object> redisTemplate) {
        this(redisTemplate, null);
    }

    // Caffeine constructor with health check
    public PluggableRedisCache(CaffeineCache caffeineCache, @Nullable RedisHealthCheck healthCheck) {
        this.redisTemplate = null;
        this.caffeineCache = caffeineCache;
        this.healthCheck = healthCheck;
        logger.info("=== PluggableRedisCache initialized with Caffeine backend ===");
    }

    // overloaded constructor for Caffeine in memory cache (backwards compatibility)
    public PluggableRedisCache(CaffeineCache caffeineCache) {
        this(caffeineCache, null);
    }

    /**
     * Determine which backend to use based on health check.
     * If health check is available, use it to determine active backend.
     * Otherwise, use the statically configured backend.
     */
    private boolean shouldUseRedis() {
        if (healthCheck != null) {
            return healthCheck.isRedisAvailable();
        }
        return redisTemplate != null;
    }

    /*
        Custom methods for the cache whether redis is available or not.
     */

    public void put(String key, Object value) {
        if (shouldUseRedis() && redisTemplate != null) {
            try {
                logger.debug("Storing in Redis - key: {}, value type: {}", cacheKeyPrefix + key, value.getClass().getSimpleName());
                redisTemplate.opsForValue().set(cacheKeyPrefix + key, value);
            } catch (Exception e) {
                logger.error("Failed to store key '{}' in Redis: {}. Falling back to Caffeine.",
                    key, e.getMessage());
                // notify health check of failure for immediate failover
                if (healthCheck != null) {
                    healthCheck.markRedisUnavailable();
                }
                // fallback to Caffeine if Redis fails
                if (caffeineCache != null && value instanceof Integer) {
                    caffeineCache.put(key, (Integer) value);
                }
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
        if (shouldUseRedis() && redisTemplate != null) {
            try {
                redisTemplate.opsForValue().set(cacheKeyPrefix + key, value, ttl);
            } catch (Exception e) {
                logger.error("Failed to store key '{}' in Redis with TTL: {}. Falling back to Caffeine.",
                    key, e.getMessage());
                // notify health check of failure for immediate failover
                if (healthCheck != null) {
                    healthCheck.markRedisUnavailable();
                }
                // failover to Caffeine if Redis fails
                if (caffeineCache != null && value instanceof Integer) {
                    caffeineCache.put(key, (Integer) value);
                }
            }
        } else if (caffeineCache != null && value instanceof Integer) {
            caffeineCache.put(key, (Integer) value);
        }
    }

    public boolean contains(String key) {
        if (shouldUseRedis() && redisTemplate != null) {
            try {
                return Boolean.TRUE.equals(redisTemplate.hasKey(cacheKeyPrefix + key));
            } catch (Exception e) {
                logger.error("Failed to check key '{}' in Redis: {}. Falling back to Caffeine.",
                    key, e.getMessage());
                // notify health check of failure for immediate failover
                if (healthCheck != null) {
                    healthCheck.markRedisUnavailable();
                }
                // failover to Caffeine if Redis fails
                if (caffeineCache != null) {
                    return caffeineCache.get(key) != null;
                }
                return false;
            }
        } else if (caffeineCache != null) {
            return caffeineCache.get(key) != null;
        }
        return false;
    }

    public Object get(String key) {
        if (shouldUseRedis() && redisTemplate != null) {
            try {
                return redisTemplate.opsForValue().get(cacheKeyPrefix + key);
            } catch (Exception e) {
                logger.error("Failed to retrieve key '{}' from Redis: {}. Falling back to Caffeine.",
                    key, e.getMessage());
                // notify health check of failure for immediate failover
                if (healthCheck != null) {
                    healthCheck.markRedisUnavailable();
                }
                // failover to Caffeine if Redis fails
                if (caffeineCache != null) {
                    return caffeineCache.get(key);
                }
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
        if (shouldUseRedis() && redisTemplate != null) {
            try {
                redisTemplate.delete(cacheKeyPrefix + key);
            } catch (Exception e) {
                logger.error("Failed to remove key '{}' from Redis: {}. Falling back to Caffeine.",
                    key, e.getMessage());
                // notify health check of failure for immediate failover
                if (healthCheck != null) {
                    healthCheck.markRedisUnavailable();
                }
                // failover to Caffeine if Redis fails
                if (caffeineCache != null) {
                    caffeineCache.remove(key);
                }
            }
        } else if (caffeineCache != null) {
            caffeineCache.remove(key);
        }
    }
}
