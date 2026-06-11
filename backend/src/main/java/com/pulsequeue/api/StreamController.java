package com.pulsequeue.api;

import com.pulsequeue.storage.MetricAggregateEntity;
import com.pulsequeue.storage.MetricAggregateRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.MediaType;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;

@RestController
@RequestMapping("/v1")
@RequiredArgsConstructor
@Slf4j
public class StreamController {

    private final MetricAggregateRepository repository;

    @GetMapping(value = "/dashboard/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<ServerSentEvent<Map<String, Object>>> streamDashboard(
            @RequestParam(defaultValue = "6h") String range) {

        return Flux.interval(Duration.ofSeconds(5))
            .onBackpressureDrop()
            .map(tick -> {
                Instant since = Instant.now().minus(6, ChronoUnit.HOURS);

                List<String> services = repository.findActiveServiceIds(
                    Instant.now().minus(24, ChronoUnit.HOURS)
                );

                List<Map<String, Object>> serviceMetrics = services.stream()
                    .map(service -> {
                        List<MetricAggregateEntity> data = repository
                            .findByServiceIdAndWindowStartAfterOrderByWindowStartAsc(
                                service, since
                            );

                        Map<String, Object> serviceData = new LinkedHashMap<>();
                        serviceData.put("service", service);

                        if (!data.isEmpty()) {
                            MetricAggregateEntity latest = data.get(data.size() - 1);
                            serviceData.put("p50", latest.getP50());
                            serviceData.put("p95", latest.getP95());
                            serviceData.put("p99", latest.getP99());
                            serviceData.put("errorRate", latest.getErrorRate());
                            serviceData.put("totalEvents", latest.getTotalEvents());
                            serviceData.put("windowStart", latest.getWindowStart().toString());
                        }

                        List<Map<String, Object>> points = data.stream().map(e -> {
                            Map<String, Object> point = new LinkedHashMap<>();
                            point.put("windowStart", e.getWindowStart().toString());
                            point.put("p50", e.getP50());
                            point.put("p95", e.getP95());
                            point.put("p99", e.getP99());
                            point.put("errorRate", e.getErrorRate());
                            return point;
                        }).toList();

                        serviceData.put("history", points);
                        return serviceData;
                    })
                    .toList();

                Map<String, Object> payload = new LinkedHashMap<>();
                payload.put("timestamp", Instant.now().toString());
                payload.put("services", serviceMetrics);

                log.debug("SSE tick: pushing {} services", serviceMetrics.size());

                return ServerSentEvent.<Map<String, Object>>builder()
                    .id(String.valueOf(tick))
                    .event("metrics")
                    .data(payload)
                    .build();
            });
    }
}
