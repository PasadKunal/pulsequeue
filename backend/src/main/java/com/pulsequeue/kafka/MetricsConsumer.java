package com.pulsequeue.kafka;

import com.pulsequeue.model.MetricAggregate;
import com.pulsequeue.storage.MetricsPersistenceService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class MetricsConsumer {

    private final MetricsPersistenceService persistenceService;

    @KafkaListener(
        topics = "${pulsequeue.kafka.topics.metrics-aggregated}",
        groupId = "pulsequeue-metrics-consumer",
        containerFactory = "kafkaListenerContainerFactory"
    )
    public void consume(MetricAggregate aggregate) {
        if (aggregate == null || aggregate.getServiceId() == null) {
            log.warn("Received null or invalid metric aggregate, skipping");
            return;
        }
        log.debug("Consuming metric aggregate: service={} events={}",
            aggregate.getServiceId(), aggregate.getTotalEvents());
        persistenceService.persist(aggregate);
    }
}
