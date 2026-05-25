package com.pulsequeue.api;

import com.pulsequeue.storage.MetricAggregateEntity;
import com.pulsequeue.storage.MetricAggregateRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;

@RestController
@RequestMapping("/v1")
@RequiredArgsConstructor
@Slf4j
public class DashboardController {

    private final MetricAggregateRepository repository;

    @GetMapping("/services")
    public ResponseEntity<Map<String, Object>> getServices() {
        List<String> services = repository.findActiveServiceIds(
            Instant.now().minus(24, ChronoUnit.HOURS)
        );
        log.debug("Returning {} services from TimescaleDB", services.size());
        return ResponseEntity.ok(Map.of("services", services));
    }

    @GetMapping("/metrics")
    public ResponseEntity<Map<String, Object>> getMetrics(
            @RequestParam String service,
            @RequestParam(defaultValue = "6h") String range) {

        Instant since = parseRange(range);
        List<MetricAggregateEntity> data = repository
            .findByServiceIdAndWindowStartAfterOrderByWindowStartAsc(service, since);

        if (data.isEmpty()) {
            return ResponseEntity.ok(Map.of(
                "service", service,
                "range", range,
                "data", List.of()
            ));
        }

        List<Map<String, Object>> points = data.stream().map(e -> {
            Map<String, Object> point = new LinkedHashMap<>();
            point.put("windowStart", e.getWindowStart().toString());
            point.put("windowEnd", e.getWindowEnd().toString());
            point.put("p50", e.getP50());
            point.put("p95", e.getP95());
            point.put("p99", e.getP99());
            point.put("errorRate", e.getErrorRate());
            point.put("totalEvents", e.getTotalEvents());
            return point;
        }).toList();

        MetricAggregateEntity latest = data.get(data.size() - 1);

        return ResponseEntity.ok(Map.of(
            "service", service,
            "range", range,
            "latest", Map.of(
                "p50", latest.getP50(),
                "p95", latest.getP95(),
                "p99", latest.getP99(),
                "errorRate", latest.getErrorRate(),
                "totalEvents", latest.getTotalEvents()
            ),
            "data", points
        ));
    }

    private Instant parseRange(String range) {
        return switch (range) {
            case "1m"  -> Instant.now().minus(1,  ChronoUnit.MINUTES);
            case "5m"  -> Instant.now().minus(5,  ChronoUnit.MINUTES);
            case "15m" -> Instant.now().minus(15, ChronoUnit.MINUTES);
            case "1h"  -> Instant.now().minus(1,  ChronoUnit.HOURS);
            case "6h"  -> Instant.now().minus(6,  ChronoUnit.HOURS);
            case "24h" -> Instant.now().minus(24, ChronoUnit.HOURS);
            default    -> Instant.now().minus(6,  ChronoUnit.HOURS);
        };
    }
}
