package com.pulsequeue.storage;

import com.pulsequeue.model.MetricAggregate;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;

@Service
@RequiredArgsConstructor
@Slf4j
public class MetricsPersistenceService {

    private final MetricAggregateRepository repository;

    @Transactional
    public void persist(MetricAggregate aggregate) {
        MetricAggregateEntity entity = MetricAggregateEntity.builder()
            .serviceId(aggregate.getServiceId())
            .windowStart(aggregate.getWindowStart())
            .windowEnd(aggregate.getWindowEnd())
            .totalEvents(aggregate.getTotalEvents())
            .errorCount(aggregate.getErrorCount())
            .errorRate(aggregate.getErrorRate())
            .p50(aggregate.getP50())
            .p95(aggregate.getP95())
            .p99(aggregate.getP99())
            .createdAt(Instant.now())
            .build();

        repository.save(entity);
        log.info("Persisted metric aggregate: service={} window={} p99={}ms",
            entity.getServiceId(),
            entity.getWindowStart(),
            entity.getP99()
        );
    }
}
