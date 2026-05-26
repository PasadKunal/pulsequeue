package com.pulsequeue.service;

import com.pulsequeue.aws.CloudWatchMetricsService;
import com.pulsequeue.aws.SqsDlqService;
import com.pulsequeue.model.IngestEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
@Slf4j
public class EventIngestionService {

    private final KafkaTemplate<String, IngestEvent> kafkaTemplate;
    private final SqsDlqService sqsDlqService;
    private final CloudWatchMetricsService cloudWatchMetricsService;

    @Value("${pulsequeue.kafka.topics.events-raw}")
    private String eventsRawTopic;

    public void ingest(List<IngestEvent> events) {
        long startMs = System.currentTimeMillis();

        for (IngestEvent event : events) {
            event.enrichTimestamp();
            kafkaTemplate.send(eventsRawTopic, event.getSourceId(), event)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.error("Failed to produce event to Kafka: source={} error={}",
                            event.getSourceId(), ex.getMessage());
                        sqsDlqService.sendToDlq(event, ex.getMessage());
                    } else {
                        log.debug("Produced event to Kafka: source={} partition={} offset={}",
                            event.getSourceId(),
                            result.getRecordMetadata().partition(),
                            result.getRecordMetadata().offset());
                    }
                });
        }

        long latencyMs = System.currentTimeMillis() - startMs;
        cloudWatchMetricsService.putKafkaProduceLatency(latencyMs);

        // publish per-service counts
        Map<String, Long> countsByService = events.stream()
            .collect(Collectors.groupingBy(IngestEvent::getSourceId, Collectors.counting()));
        countsByService.forEach((serviceId, count) ->
            cloudWatchMetricsService.putEventsIngested(count.intValue(), serviceId));

        log.info("Dispatched {} events to Kafka topic {}", events.size(), eventsRawTopic);
    }
}
