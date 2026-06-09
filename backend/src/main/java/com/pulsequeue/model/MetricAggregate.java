package com.pulsequeue.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class MetricAggregate {

    private String serviceId;
    private Instant windowStart;
    private Instant windowEnd;
    private long totalEvents;
    private long errorCount;
    private double errorRate;
    private long p50;
    private long p95;
    private long p99;

    private double zScoreP99;
    private double zScoreErrorRate;
    private boolean anomalyDetected;
    private String anomalyReason;

    @Builder.Default
    private List<Long> latencies = new ArrayList<>();

    public void addEvent(IngestEvent event) {
        totalEvents++;
        if (event.isError()) {
            errorCount++;
        }
        if (event.getLatencyMs() != null) {
            latencies.add(event.getLatencyMs());
        }
    }

    public void computePercentiles() {
        if (latencies.isEmpty()) return;

        Collections.sort(latencies);
        int size = latencies.size();

        p50 = latencies.get((int) Math.ceil(size * 0.50) - 1);
        p95 = latencies.get((int) Math.ceil(size * 0.95) - 1);
        p99 = latencies.get((int) Math.ceil(size * 0.99) - 1);

        errorRate = totalEvents > 0 ? (double) errorCount / totalEvents : 0.0;
    }
}
