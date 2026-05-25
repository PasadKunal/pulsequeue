package com.pulsequeue.service;

import com.pulsequeue.model.IngestEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
@Slf4j
public class EventIngestionService {

    public void ingest(List<IngestEvent> events) {
        for (IngestEvent event : events) {
            event.enrichTimestamp();
            log.debug("Received event: source={} type={} latencyMs={} statusCode={} timestamp={}",
                event.getSourceId(),
                event.getEventType(),
                event.getLatencyMs(),
                event.getStatusCode(),
                event.getTimestamp()
            );
        }
        log.info("Ingested {} events", events.size());
    }
}
