package net.damero.Kafka.Config;

import net.damero.Kafka.Aspect.Components.CaffeineCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.RedisTemplate;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.springframework.beans.factory.annotation.Value;

/**
 * Monitors Redis health and automatically switches between Redis and Caffeine caches.
 * Performs health checks every 1 seconds to detect Redis availability.
 */
public class RedisHealthCheck {

    private static final Logger logger = LoggerFactory.getLogger(RedisHealthCheck.class);

    @Value("${kafka.damero.redis.health-check-interval:10}")
    private static final long HEALTH_CHECK_INTERVAL_SECONDS = 10;
    
    private static final int REQUIRED_CONSECUTIVE_SUCCESSES = 3;

    private final RedisTemplate<String, Object> redisTemplate;
    private final AtomicBoolean redisAvailable = new AtomicBoolean(false);
    private int consecutiveSuccesses = 0;
    private ScheduledExecutorService healthCheckScheduler;

    public RedisHealthCheck(RedisTemplate<String, Object> redisTemplate,
                           CaffeineCache caffeineCache) {
        this.redisTemplate = redisTemplate;

        // Determine initial state
        boolean initialState = testRedisConnection();
        this.redisAvailable.set(initialState);

        if (!initialState) {
            logger.debug("Redis health check failed: Unable to connect to Redis");
        }
    }

    @PostConstruct
    public void startHealthCheck() {
        healthCheckScheduler = Executors.newScheduledThreadPool(1, r -> {
            Thread thread = new Thread(r, "redis-health-check");
            thread.setDaemon(true);
            return thread;
        });

        healthCheckScheduler.scheduleAtFixedRate(
            this::performHealthCheck,
            HEALTH_CHECK_INTERVAL_SECONDS,
            HEALTH_CHECK_INTERVAL_SECONDS,
            TimeUnit.SECONDS
        );

        logger.info("Redis health check started - checking every {} seconds", HEALTH_CHECK_INTERVAL_SECONDS);
    }

    @PreDestroy
    public void stopHealthCheck() {
        if (healthCheckScheduler != null) {
            healthCheckScheduler.shutdown();
            try {
                if (!healthCheckScheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                    healthCheckScheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                healthCheckScheduler.shutdownNow();
                Thread.currentThread().interrupt();
            }
            logger.info("Redis health check stopped");
        }
    }

    private void performHealthCheck() {
        boolean currentlyAvailable = testRedisConnection();
        boolean previouslyAvailable = redisAvailable.get();

        if (currentlyAvailable) {
            consecutiveSuccesses++;

            // Only switch to Redis after multiple consecutive successful checks
            if (!previouslyAvailable && consecutiveSuccesses >= REQUIRED_CONSECUTIVE_SUCCESSES) {
                redisAvailable.set(true);
                logger.warn("==> Redis connection RESTORED - switching from Caffeine to Redis backend");
            }

            logger.debug("Redis health check: HEALTHY (consecutive: {})", consecutiveSuccesses);
        } else {
            // Reset counter on failure
            consecutiveSuccesses = 0;

            if (previouslyAvailable) {
                // Redis went offline
                redisAvailable.set(false);
                logger.error("==> Redis connection LOST - switching from Redis to Caffeine backend");
                logger.warn("Active cache switched to Caffeine backend - multi-instance deduplication disabled");
            } else {
                logger.debug("Redis health check: UNAVAILABLE (using Caffeine)");
            }
        }
    }

    private boolean testRedisConnection() {
        if (redisTemplate == null) {
            return false;
        }

        try {
            // Quick timeout check - use a fast ping with timeout
            String result = java.util.concurrent.CompletableFuture.supplyAsync(() -> {
                try {
                    return redisTemplate.execute((org.springframework.data.redis.core.RedisCallback<String>) connection -> {
                        try {
                            // Ping command should return PONG if connection is healthy
                            return connection.ping();
                        } catch (Exception e) {
                            logger.debug("Redis ping failed: {}", e.getMessage());
                            return null;
                        }
                    });
                } catch (Exception e) {
                    return null;
                }
            }).get(500, java.util.concurrent.TimeUnit.MILLISECONDS); // 500ms timeout

            if ("PONG".equals(result)) {
                // Quick validation with a simple set/get operation (also with timeout)
                try {
                    String testKey = "health_check_test";
                    redisTemplate.opsForValue().set(testKey, "test", java.time.Duration.ofSeconds(5));
                    String testValue = (String) redisTemplate.opsForValue().get(testKey);
                    redisTemplate.delete(testKey);
                    return "test".equals(testValue);
                } catch (Exception e) {
                    logger.debug("Redis operation test failed: {}", e.getMessage());
                    return false;
                }
            }
            return false;
        } catch (java.util.concurrent.TimeoutException e) {
            logger.debug("Redis health check failed: Connection timeout");
            return false;
        } catch (org.springframework.data.redis.RedisConnectionFailureException e) {
            logger.debug("Redis health check failed: Unable to connect to Redis - {}", e.getMessage());
            return false;
        } catch (org.springframework.data.redis.RedisSystemException e) {
            logger.debug("Redis health check failed: Redis system error - {}", e.getMessage());
            return false;
        } catch (Exception e) {
            String message = e.getMessage();
            if (message != null && message.contains("LettuceConnectionFactory")) {
                logger.debug("Redis health check failed: {}", message.split("\n")[0]);
            } else if (message != null && (message.contains("timed out") || message.contains("timeout"))) {
                logger.debug("Redis health check failed: Redis command timed out");
            } else if (message != null && (message.contains("Connection refused") || message.contains("connection closed"))) {
                logger.debug("Redis health check failed: Connection refused or closed");
            } else {
                logger.debug("Redis health check failed: Unknown redis exception - {}", e.getClass().getSimpleName());
            }
            return false;
        }
    }


    /**
     * Check if Redis is currently available.
     */
    public boolean isRedisAvailable() {
        return redisAvailable.get();
    }

    /**
     * Force an immediate health check (useful for testing).
     */
    public void checkNow() {
        performHealthCheck();
    }

    /**
     * Mark Redis as unavailable immediately when an operation fails.
     * This allows for faster failover when Redis operations throw exceptions.
     */
    public void markRedisUnavailable() {
        boolean wasAvailable = redisAvailable.getAndSet(false);
        if (wasAvailable) {
            consecutiveSuccesses = 0;
            logger.error("==> Redis operation failed - immediately switching from Redis to Caffeine backend");
            logger.warn("Active cache switched to Caffeine backend - multi-instance deduplication disabled");
        }
    }
}
