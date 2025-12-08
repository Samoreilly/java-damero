package net.damero.Kafka.Config;

import net.damero.Kafka.Aspect.Components.CaffeineCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.lang.Nullable;

import java.time.Duration;

/**
 * Cache implementation that uses Redis when available and falls back to Caffeine.
 *
 * CRITICAL WARNING: In multi-instance deployments, if Redis fails and strictMode=false,
 * different instances will use separate Caffeine caches causing:
 * - Duplicate message processing (deduplication breaks)
 * - Incorrect retry counts (each instance tracks separately)
 * - Inconsistent circuit breaker states
 *
 * RECOMMENDATION: Always use strictMode=true in production (default).
 * This will fail fast when Redis is unavailable instead of silently degrading.
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

    // ==================== Constructors ====================

    /**
     * Primary constructor with all options.
     */
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

    /**
     * Constructor with Redis + Caffeine, strict mode enabled by default.
     */
    public PluggableRedisCache(RedisTemplate<String, Object> redisTemplate,
                              CaffeineCache caffeineCache,
                              @Nullable RedisHealthCheck healthCheck) {
        this(redisTemplate, caffeineCache, healthCheck, true);
    }

    /**
     * Redis-only constructor with health check.
     */
    public PluggableRedisCache(RedisTemplate<String, Object> redisTemplate, @Nullable RedisHealthCheck healthCheck) {
        this.redisTemplate = redisTemplate;
        this.caffeineCache = null;
        this.healthCheck = healthCheck;
        this.strictMode = true;
        if (redisTemplate != null) {
            logger.info("=== PluggableRedisCache initialized with Redis backend (no failover) ===");
        }
    }

    /**
     * Redis-only constructor (backwards compatibility).
     */
    public PluggableRedisCache(RedisTemplate<String, Object> redisTemplate) {
        this(redisTemplate, (RedisHealthCheck) null);
    }

    /**
     * Caffeine-only constructor with health check.
     */
    public PluggableRedisCache(CaffeineCache caffeineCache, @Nullable RedisHealthCheck healthCheck) {
        this.redisTemplate = null;
        this.caffeineCache = caffeineCache;
        this.healthCheck = healthCheck;
        this.strictMode = false;
        logger.info("=== PluggableRedisCache initialized with Caffeine backend ===");
    }

    /**
     * Caffeine-only constructor (backwards compatibility).
     */
    public PluggableRedisCache(CaffeineCache caffeineCache) {
        this(caffeineCache, null);
    }

    // ==================== Core Cache Operations ====================

    /**
     * Determine which backend to use based on health check.
     */
    private boolean shouldUseRedis() {
        if (healthCheck != null) {
            return healthCheck.isRedisAvailable();
        }
        return redisTemplate != null;
    }

    /**
     * Store a value in the cache.
     */
    public void put(String key, Object value) {
        if (shouldUseRedis() && redisTemplate != null) {
            try {
                logger.debug("Storing in Redis - key: {}", cacheKeyPrefix + key);
                redisTemplate.opsForValue().set(cacheKeyPrefix + key, value);
            } catch (Exception e) {
                handleRedisFailure("put", key, e);
                if (caffeineCache != null && value instanceof Integer) {
                    caffeineCache.put(key, (Integer) value);
                }
            }
        } else if (caffeineCache != null && value instanceof Integer) {
            caffeineCache.put(key, (Integer) value);
        }
    }

    /**
     * Store a value in the cache with TTL.
     */
    public void put(String key, Object value, Duration ttl) {
        if (shouldUseRedis() && redisTemplate != null) {
            try {
                redisTemplate.opsForValue().set(cacheKeyPrefix + key, value, ttl);
            } catch (Exception e) {
                handleRedisFailure("put with TTL", key, e);
                if (caffeineCache != null && value instanceof Integer) {
                    caffeineCache.put(key, (Integer) value);
                }
            }
        } else if (caffeineCache != null && value instanceof Integer) {
            caffeineCache.put(key, (Integer) value);
        }
    }

    /**
     * Check if a key exists in the cache.
     */
    public boolean contains(String key) {
        if (shouldUseRedis() && redisTemplate != null) {
            try {
                Boolean exists = redisTemplate.hasKey(cacheKeyPrefix + key);
                return Boolean.TRUE.equals(exists);
            } catch (Exception e) {
                handleRedisFailure("contains", key, e);
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

    /**
     * Get a value from the cache.
     */
    public Object get(String key) {
        if (shouldUseRedis() && redisTemplate != null) {
            try {
                return redisTemplate.opsForValue().get(cacheKeyPrefix + key);
            } catch (Exception e) {
                handleRedisFailure("get", key, e);
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

    /**
     * Get an Integer value, returning default if not found.
     * Does NOT store the default value.
     */
    public Integer getOrDefault(String key, Integer defaultValue) {
        Object value = get(key);
        if (value instanceof Integer) {
            return (Integer) value;
        }
        if (value instanceof Long) {
            return ((Long) value).intValue();
        }
        return defaultValue;
    }

    /**
     * Remove a key from the cache.
     */
    public void remove(String key) {
        if (shouldUseRedis() && redisTemplate != null) {
            try {
                redisTemplate.delete(cacheKeyPrefix + key);
            } catch (Exception e) {
                handleRedisFailure("remove", key, e);
                if (caffeineCache != null) {
                    caffeineCache.remove(key);
                }
            }
        } else if (caffeineCache != null) {
            caffeineCache.remove(key);
        }
    }

    /**
     * Atomically increment a counter and return the new value.
     * Uses Redis INCR for atomicity across instances.
     */
    public int incrementAndGet(String key) {
        if (shouldUseRedis() && redisTemplate != null) {
            try {
                logger.debug("Incrementing key in Redis: {}", cacheKeyPrefix + key);
                Long newValue = redisTemplate.opsForValue().increment(cacheKeyPrefix + key);
                if (newValue == null) {
                    throw new IllegalStateException("Redis INCR returned null for key: " + key);
                }
                return newValue.intValue();
            } catch (Exception e) {
                handleRedisFailure("increment", key, e);
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

    // ==================== Rate Limiting ====================

    private static final String RATE_LIMIT_PREFIX = "ratelimit:";
    private static final String COUNTER_FIELD = "count";
    private static final String WINDOW_START_FIELD = "windowStart";

    /**
     * Increment the rate limit counter for a topic.
     */
    public long incrementRateLimitCounter(String topic) {
        String key = cacheKeyPrefix + RATE_LIMIT_PREFIX + topic;

        if (shouldUseRedis() && redisTemplate != null) {
            try {
                Long count = redisTemplate.opsForHash().increment(key, COUNTER_FIELD, 1);
                return count != null ? count : 1;
            } catch (Exception e) {
                handleRedisFailure("incrementRateLimitCounter", topic, e);
            }
        }
        // Caffeine fallback
        if (caffeineCache != null) {
            String counterKey = RATE_LIMIT_PREFIX + topic + ":count";
            Integer current = caffeineCache.get(counterKey);
            int newValue = (current == null ? 0 : current) + 1;
            caffeineCache.put(counterKey, newValue);
            return newValue;
        }
        return 1;
    }

    /**
     * Get the current window start time for a topic.
     */
    public long getRateLimitWindowStart(String topic) {
        String key = cacheKeyPrefix + RATE_LIMIT_PREFIX + topic;

        if (shouldUseRedis() && redisTemplate != null) {
            try {
                Object value = redisTemplate.opsForHash().get(key, WINDOW_START_FIELD);
                if (value instanceof Number) {
                    return ((Number) value).longValue();
                }
                if (value instanceof String) {
                    return Long.parseLong((String) value);
                }
            } catch (Exception e) {
                handleRedisFailure("getRateLimitWindowStart", topic, e);
            }
        }
        // Caffeine fallback
        if (caffeineCache != null) {
            String windowKey = RATE_LIMIT_PREFIX + topic + ":window";
            Integer value = caffeineCache.get(windowKey);
            return value != null ? value * 1000L : 0;
        }
        return 0;
    }

    /**
     * Reset the rate limit window for a topic.
     */
    public void resetRateLimitWindow(String topic, long windowStartTime) {
        String key = cacheKeyPrefix + RATE_LIMIT_PREFIX + topic;

        if (shouldUseRedis() && redisTemplate != null) {
            try {
                redisTemplate.opsForHash().put(key, COUNTER_FIELD, 1);
                redisTemplate.opsForHash().put(key, WINDOW_START_FIELD, windowStartTime);
                return;
            } catch (Exception e) {
                handleRedisFailure("resetRateLimitWindow", topic, e);
            }
        }
        // Caffeine fallback
        if (caffeineCache != null) {
            String counterKey = RATE_LIMIT_PREFIX + topic + ":count";
            String windowKey = RATE_LIMIT_PREFIX + topic + ":window";
            caffeineCache.put(counterKey, 1);
            caffeineCache.put(windowKey, (int) (windowStartTime / 1000));
        }
    }

    // ==================== Fibonacci State Management ====================

    private static final String FIBONACCI_PREFIX = "fib:";

    /**
     * Gets the next Fibonacci delay value for an event and advances the state.
     * This is atomic across distributed instances when using Redis.
     *
     * @param eventId the event identifier
     * @param fibonacciLimit the maximum fibonacci sequence index
     * @return the next fibonacci delay value in the sequence
     */
    public long getNextFibonacciDelay(String eventId, int fibonacciLimit) {
        String key = FIBONACCI_PREFIX + eventId;

        // Get current index and increment atomically
        int currentIndex = incrementAndGetFibonacciIndex(key);

        // Calculate fibonacci value for this index (capped at limit)
        int effectiveIndex = Math.min(currentIndex, fibonacciLimit - 1);
        return calculateFibonacci(effectiveIndex);
    }

    /**
     * Atomically increment the fibonacci index and return the new value.
     */
    private int incrementAndGetFibonacciIndex(String key) {
        if (shouldUseRedis() && redisTemplate != null) {
            try {
                Long newValue = redisTemplate.opsForValue().increment(cacheKeyPrefix + key);
                // Start from index 2 (first computed fibonacci after 0,1)
                return newValue != null ? newValue.intValue() + 1 : 2;
            } catch (Exception e) {
                handleRedisFailure("incrementFibonacciIndex", key, e);
                return incrementFibonacciIndexCaffeine(key);
            }
        }
        return incrementFibonacciIndexCaffeine(key);
    }

    /**
     * Caffeine fallback for fibonacci index increment.
     */
    private int incrementFibonacciIndexCaffeine(String key) {
        if (caffeineCache != null) {
            Integer current = caffeineCache.get(FIBONACCI_PREFIX + key);
            int newValue = (current == null ? 1 : current) + 1;
            caffeineCache.put(FIBONACCI_PREFIX + key, newValue);
            return newValue + 1; // +1 to start from index 2
        }
        return 2; // Default starting index
    }

    /**
     * Calculate fibonacci value at given index.
     * Uses iterative approach to avoid stack overflow for large indices.
     */
    private long calculateFibonacci(int index) {
        if (index <= 0) return 0;
        if (index == 1) return 1;

        long prev = 0;
        long curr = 1;
        for (int i = 2; i <= index; i++) {
            long next = prev + curr;
            prev = curr;
            curr = next;
        }
        return curr;
    }

    /**
     * Clear the fibonacci state for an event.
     * Call this when event processing completes successfully.
     *
     * @param eventId the event identifier
     */
    public void clearFibonacciState(String eventId) {
        if (eventId == null) return;

        String key = FIBONACCI_PREFIX + eventId;
        remove(key);
        logger.debug("Cleared fibonacci state for event: {}", eventId);
    }

    // ==================== Error Handling ====================

    /**
     * Common error handling for Redis failures.
     */
    private void handleRedisFailure(String operation, String key, Exception e) {
        logger.error("Failed to {} key '{}' in Redis: {}", operation, key, e.getMessage());

        if (healthCheck != null) {
            healthCheck.markRedisUnavailable();
        }

        if (strictMode) {
            throw new CacheUnavailableException(STRICT_MODE_ERROR, e);
        }

        logger.warn("DEGRADED MODE: Falling back to Caffeine cache for {} operation", operation);
    }

    /**
     * Exception thrown when cache operations fail and strict mode is enabled.
     */
    public static class CacheUnavailableException extends RuntimeException {
        public CacheUnavailableException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}

