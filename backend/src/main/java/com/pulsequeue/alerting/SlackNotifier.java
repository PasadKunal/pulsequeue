package com.pulsequeue.alerting;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.util.Map;

@Service
@Slf4j
public class SlackNotifier {

    @Value("${pulsequeue.alerting.slack-webhook-url:#{null}}")
    private String webhookUrl;

    private final RestTemplate restTemplate = new RestTemplate();

    public void send(String serviceId, String metric, double currentValue, double threshold, String operator) {
        if (webhookUrl == null || webhookUrl.isBlank()) {
            log.warn("Slack webhook URL not configured - skipping notification");
            return;
        }

        String emoji = metric.equals("error_rate") ? ":red_circle:" : ":warning:";
        String formattedValue = metric.equals("error_rate")
            ? String.format("%.1f%%", currentValue * 100)
            : String.format("%.0fms", currentValue);
        String formattedThreshold = metric.equals("error_rate")
            ? String.format("%.1f%%", threshold * 100)
            : String.format("%.0fms", threshold);

        String text = String.format(
            "%s *PulseQueue Alert*\n" +
            ">*Service:* %s\n" +
            ">*Metric:* %s\n" +
            ">*Current value:* %s\n" +
            ">*Threshold:* %s %s\n",
            emoji, serviceId, metric, formattedValue, operator, formattedThreshold
        );

        try {
            restTemplate.postForObject(webhookUrl, Map.of("text", text), String.class);
            log.info("Slack alert sent: service={} metric={} value={} threshold={}",
                serviceId, metric, formattedValue, formattedThreshold);
        } catch (Exception e) {
            log.error("Failed to send Slack alert: {}", e.getMessage());
        }
    }
}
