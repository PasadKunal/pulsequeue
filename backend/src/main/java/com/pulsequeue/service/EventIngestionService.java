package com.pulsequeue.service;

import com.pulsequeue.model.IngestEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
@RequiredArgsConstructor
@Slf4j
public class EventIngestionService {

    private final KafkaTemplate<String, IngestEvent> kafkaTemplate;

    @Value("${pulsequeue.kafka.topics.events-raw}")
    private String eventsRawTopic;

    public void ingest(List<IngestEvent> events) {
        for (IngestEvent event : events) {
            event.enrichTimestamp();
            kafkaTemplate.send(eventsRawTopic, event.getSourceId(), event)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.error("Failed to produce event to Kafka: source={} error={}",
                            event.getSourceId(), ex.getMessage());
                    } else {
                        log.debug("Produced event to Kafka: source={} partition={} offset={}",
                            event.getSourceId(),
                            result.getRecordMetadata().partition(),
                            result.getRecordMetadata().offset());
                    }
                });
        }
        log.info("Dispatched {} events to Kafka topic {}", events.size(), eventsRawTopic);
    }
}
