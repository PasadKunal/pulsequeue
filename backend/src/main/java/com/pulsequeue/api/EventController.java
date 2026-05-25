package com.pulsequeue.api;

import com.pulsequeue.model.IngestEvent;
import com.pulsequeue.service.EventIngestionService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/v1")
@RequiredArgsConstructor
@Slf4j
public class EventController {

    private final EventIngestionService ingestionService;

    @PostMapping("/events")
    public ResponseEntity<Map<String, Object>> ingestEvents(
            @RequestBody List<IngestEvent> events) {

        if (events == null || events.isEmpty()) {
            return ResponseEntity.badRequest()
                .body(Map.of("error", "request body must contain at least one event"));
        }

        if (events.size() > 500) {
            return ResponseEntity.badRequest()
                .body(Map.of("error", "batch size exceeds maximum of 500 events"));
        }

        ingestionService.ingest(events);

        return ResponseEntity.ok(Map.of(
            "received", events.size(),
            "status", "accepted"
        ));
    }
}
