package net.damero.Kafka.Config;

import net.damero.Kafka.Aspect.Components.CaffeineCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.lang.Nullable;

import java.time.Duration;

/**
 * cache implementation that uses redis when available and falls back to caffeine
 *
 * CRITICAL WARNING: in multi-instance deployments, if redis fails and strictMode=false,
 * different instances will use separate caffeine caches causing:
 * - duplicate message processing (deduplication breaks)
 * - incorrect retry counts (each instance tracks separately)
 * - inconsistent circuit breaker states
 *
 * RECOMMENDATION: always use strictMode=true in production (default)
 * this will fail fast when redis is unavailable instead of silently degrading
 */
public class PluggableRedisCache {

    private static final Logger logger = LoggerFactory.getLogger(PluggableRedisCache.class);

    private final RedisTemplate<String, Object> redisTemplate;
    private final CaffeineCache caffeineCache;
    private final RedisHealthCheck healthCheck;
    private final String cacheKeyPrefix = "internal_cache:";
    private final boolean strictMode;

    private static final String STRICT_MODE_ERROR =
        "Redis is unavailable and strict mode is enabled. " +
        "This prevents split-brain scenarios in multi-instance deployments. " +
        "Set damero.cache.strict-mode=false to allow degradation to Caffeine (NOT recommended for production).";

    // Constructor with both Redis and Caffeine backends plus health check (for automatic failover)
    public PluggableRedisCache(RedisTemplate<String, Object> redisTemplate,
                              CaffeineCache caffeineCache,
                              @Nullable RedisHealthCheck healthCheck,
                              boolean strictMode) {
        this.redisTemplate = redisTemplate;
        this.caffeineCache = caffeineCache;
        this.healthCheck = healthCheck;
        this.strictMode = strictMode;

        if (strictMode) {
            logger.info("=== PluggableRedisCache initialized with Redis + Caffeine failover (STRICT MODE ENABLED) ===");
            logger.info("=== Redis failures will throw exceptions to prevent split-brain scenarios ===");
        } else {
            logger.warn("=== PluggableRedisCache initialized with Redis + Caffeine failover (STRICT MODE DISABLED) ===");
            logger.warn("=== WARNING: Redis failures will silently degrade to Caffeine - this can cause data inconsistency ===");
        }
    }

    // backwards compatibility constructors
    public PluggableRedisCache(RedisTemplate<String, Object> redisTemplate,
                              CaffeineCache caffeineCache,
                              @Nullable RedisHealthCheck healthCheck) {
        this(redisTemplate, caffeineCache, healthCheck, true); // default to strict mode
    }

    // Redis constructor with health check
    public PluggableRedisCache(RedisTemplate<String, Object> redisTemplate, @Nullable RedisHealthCheck healthCheck) {
        this.redisTemplate = redisTemplate;
        this.caffeineCache = null;
        this.healthCheck = healthCheck;
        this.strictMode = true; // redis-only is always strict
        if (redisTemplate != null) {
            logger.info("=== PluggableRedisCache initialized with Redis backend (no failover) ===");
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
        this.strictMode = false; // caffeine-only can't be strict
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
                logger.error("Failed to store key '{}' in Redis: {}", key, e.getMessage());
                // notify health check of failure for immediate failover
                if (healthCheck != null) {
                    healthCheck.markRedisUnavailable();
                }

                // in strict mode, fail fast to prevent split-brain
                if (strictMode) {
                    throw new CacheUnavailableException(STRICT_MODE_ERROR, e);
                }

                // fallback to Caffeine if Redis fails and strict mode is disabled
                logger.warn("DEGRADED MODE: Falling back to Caffeine cache - may cause data inconsistency in multi-instance deployments");
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
                logger.error("Failed to store key '{}' in Redis with TTL: {}", key, e.getMessage());
                // notify health check of failure for immediate failover
                if (healthCheck != null) {
                    healthCheck.markRedisUnavailable();
                }

                // in strict mode, fail fast to prevent split-brain
                if (strictMode) {
                    throw new CacheUnavailableException(STRICT_MODE_ERROR, e);
                }

                // failover to Caffeine if Redis fails and strict mode is disabled
                logger.warn("DEGRADED MODE: Falling back to Caffeine cache - may cause data inconsistency");
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
                logger.error("Failed to check key '{}' in Redis: {}", key, e.getMessage());
                // notify health check of failure for immediate failover
                if (healthCheck != null) {
                    healthCheck.markRedisUnavailable();
                }

                // in strict mode, fail fast
                if (strictMode) {
                    throw new CacheUnavailableException(STRICT_MODE_ERROR, e);
                }

                // failover to Caffeine if Redis fails
                logger.warn("DEGRADED MODE: Falling back to Caffeine cache for contains check");
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
                logger.error("Failed to retrieve key '{}' from Redis: {}", key, e.getMessage());
                // notify health check of failure for immediate failover
                if (healthCheck != null) {
                    healthCheck.markRedisUnavailable();
                }

                // in strict mode, fail fast
                if (strictMode) {
                    throw new CacheUnavailableException(STRICT_MODE_ERROR, e);
                }

                // failover to Caffeine if Redis fails
                logger.warn("DEGRADED MODE: Falling back to Caffeine cache for get operation");
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
                logger.error("Failed to remove key '{}' from Redis: {}", key, e.getMessage());
                // notify health check of failure for immediate failover
                if (healthCheck != null) {
                    healthCheck.markRedisUnavailable();
                }

                // in strict mode, fail fast
                if (strictMode) {
                    throw new CacheUnavailableException(STRICT_MODE_ERROR, e);
                }

                // failover to Caffeine if Redis fails
                logger.warn("DEGRADED MODE: Falling back to Caffeine cache for remove operation");
                if (caffeineCache != null) {
                    caffeineCache.remove(key);
                }
            }
        } else if (caffeineCache != null) {
            caffeineCache.remove(key);
        }
    }

    /**
     * atomically increment a counter and return the new value
     * this is critical for retry counting to prevent race conditions
     *
     * IMPORTANT: this uses Redis INCR command for atomicity when redis is available
     * when using caffeine, this falls back to caffeine's atomic operations
     *
     * @param key the counter key
     * @return the new incremented value
     */
    public int incrementAndGet(String key) {
        if (shouldUseRedis() && redisTemplate != null) {
            try {
                Long newValue = redisTemplate.opsForValue().increment(cacheKeyPrefix + key);
                if (newValue == null) {
                    throw new IllegalStateException("Redis INCR returned null for key: " + key);
                }
                return newValue.intValue();
            } catch (Exception e) {
                logger.error("Failed to increment key '{}' in Redis: {}", key, e.getMessage());
                // notify health check of failure
                if (healthCheck != null) {
                    healthCheck.markRedisUnavailable();
                }

                // in strict mode, fail fast
                if (strictMode) {
                    throw new CacheUnavailableException(STRICT_MODE_ERROR, e);
                }

                // fallback to caffeine with warning
                logger.warn("DEGRADED MODE: Falling back to Caffeine for increment - NOT ATOMIC across instances");
                if (caffeineCache != null) {
                    Integer current = caffeineCache.get(key);
                    int newValue = (current == null ? 0 : current) + 1;
                    caffeineCache.put(key, newValue);
                    return newValue;
                }
                return 1;
            }
        } else if (caffeineCache != null) {
            Integer current = caffeineCache.get(key);
            int newValue = (current == null ? 0 : current) + 1;
            caffeineCache.put(key, newValue);
            return newValue;
        }
        return 1;
    }

    /**
     * exception thrown when cache operations fail and strict mode is enabled
     * this prevents split-brain scenarios in multi-instance deployments
     */
    public static class CacheUnavailableException extends RuntimeException {
        public CacheUnavailableException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
