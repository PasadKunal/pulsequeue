package com.pulsequeue.storage;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

@Entity
@Table(name = "metric_aggregates")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class MetricAggregateEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "service_id", nullable = false)
    private String serviceId;

    @Column(name = "window_start", nullable = false)
    private Instant windowStart;

    @Column(name = "window_end", nullable = false)
    private Instant windowEnd;

    @Column(name = "total_events", nullable = false)
    private Long totalEvents;

    @Column(name = "error_count", nullable = false)
    private Long errorCount;

    @Column(name = "error_rate", nullable = false)
    private Double errorRate;

    @Column(name = "p50", nullable = false)
    private Long p50;

    @Column(name = "p95", nullable = false)
    private Long p95;

    @Column(name = "p99", nullable = false)
    private Long p99;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;
}
