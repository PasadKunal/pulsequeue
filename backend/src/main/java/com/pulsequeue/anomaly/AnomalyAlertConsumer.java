package com.pulsequeue.anomaly;

import com.pulsequeue.alerting.SlackNotifier;
import com.pulsequeue.model.MetricAggregate;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class AnomalyAlertConsumer {

    private final SlackNotifier slackNotifier;

    @KafkaListener(
        topics = "${pulsequeue.kafka.topics.anomalies}",
        groupId = "pulsequeue-anomaly-alerts",
        containerFactory = "kafkaListenerContainerFactory"
    )
    public void onAnomaly(MetricAggregate aggregate) {
        log.info("Anomaly event received: service={} zP99={} zErr={} reason={}",
            aggregate.getServiceId(),
            String.format("%.2f", aggregate.getZScoreP99()),
            String.format("%.2f", aggregate.getZScoreErrorRate()),
            aggregate.getAnomalyReason());

        slackNotifier.sendAnomaly(
            aggregate.getServiceId(),
            aggregate.getZScoreP99(),
            aggregate.getZScoreErrorRate(),
            aggregate.getAnomalyReason()
        );
    }
}
