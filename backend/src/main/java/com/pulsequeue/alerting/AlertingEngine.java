package com.pulsequeue.alerting;

import com.pulsequeue.storage.MetricAggregateEntity;
import com.pulsequeue.storage.MetricAggregateRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;

@Service
@RequiredArgsConstructor
@Slf4j
public class AlertingEngine {

    private final AlertRuleRepository alertRuleRepository;
    private final MetricAggregateRepository metricRepository;
    private final SlackNotifier slackNotifier;

    @Scheduled(fixedDelay = 30000)
    public void evaluate() {
        List<AlertRule> rules = alertRuleRepository.findByEnabledTrue();
        if (rules.isEmpty()) return;

        log.debug("Evaluating {} alert rules", rules.size());

        Instant since = Instant.now().minus(2, ChronoUnit.MINUTES);

        for (AlertRule rule : rules) {
            List<MetricAggregateEntity> recent = metricRepository
                .findByServiceIdAndWindowStartAfterOrderByWindowStartAsc(
                    rule.getServiceId(), since
                );

            if (recent.isEmpty()) continue;

            MetricAggregateEntity latest = recent.get(recent.size() - 1);
            double currentValue = extractMetric(latest, rule.getMetric());

            if (rule.evaluate(currentValue)) {
                log.warn("ALERT BREACH: service={} metric={} value={} threshold={} operator={}",
                    rule.getServiceId(), rule.getMetric(),
                    currentValue, rule.getThreshold(), rule.getOperator());

                slackNotifier.send(
                    rule.getServiceId(),
                    rule.getMetric(),
                    currentValue,
                    rule.getThreshold(),
                    rule.getOperator()
                );
            }
        }
    }

    private double extractMetric(MetricAggregateEntity entity, String metric) {
        return switch (metric.toLowerCase()) {
            case "p50"        -> entity.getP50();
            case "p95"        -> entity.getP95();
            case "p99"        -> entity.getP99();
            case "error_rate" -> entity.getErrorRate();
            default -> 0.0;
        };
    }
}
