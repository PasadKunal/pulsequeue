package com.pulsequeue.alerting;

import jakarta.persistence.*;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

@Entity
@Table(name = "alert_rules")
@Data
@NoArgsConstructor
public class AlertRule {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "service_id", nullable = false)
    private String serviceId;

    @Column(name = "metric", nullable = false)
    private String metric;

    @Column(name = "operator", nullable = false)
    private String operator;

    @Column(name = "threshold", nullable = false)
    private Double threshold;

    @Column(name = "enabled", nullable = false)
    private Boolean enabled = true;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    public boolean evaluate(double value) {
        return switch (operator.toUpperCase()) {
            case "GT" -> value > threshold;
            case "LT" -> value < threshold;
            case "GTE" -> value >= threshold;
            case "LTE" -> value <= threshold;
            default -> false;
        };
    }
}
