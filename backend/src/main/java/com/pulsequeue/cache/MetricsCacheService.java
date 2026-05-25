package com.pulsequeue.cache;

import com.pulsequeue.storage.MetricAggregateEntity;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.Optional;

@Service
@RequiredArgsConstructor
@Slf4j
public class MetricsCacheService {

    private final RedisTemplate<String, Object> redisTemplate;

    private static final Duration TTL = Duration.ofSeconds(30);
    private static final String LATEST_KEY_PREFIX = "metrics:latest:";
    private static final String SERVICES_KEY = "metrics:services";

    public void cacheLatest(MetricAggregateEntity entity) {
        String key = LATEST_KEY_PREFIX + entity.getServiceId();
        redisTemplate.opsForValue().set(key, entity, TTL);
        redisTemplate.opsForSet().add(SERVICES_KEY, entity.getServiceId());
        redisTemplate.expire(SERVICES_KEY, Duration.ofMinutes(5));
        log.debug("Cached latest metrics for service={} p99={}ms",
            entity.getServiceId(), entity.getP99());
    }

    public Optional<MetricAggregateEntity> getLatest(String serviceId) {
        String key = LATEST_KEY_PREFIX + serviceId;
        Object value = redisTemplate.opsForValue().get(key);
        if (value == null) return Optional.empty();
        if (value instanceof MetricAggregateEntity entity) {
            return Optional.of(entity);
        }
        return Optional.empty();
    }

    public java.util.Set<Object> getActiveServices() {
        return redisTemplate.opsForSet().members(SERVICES_KEY);
    }
}
