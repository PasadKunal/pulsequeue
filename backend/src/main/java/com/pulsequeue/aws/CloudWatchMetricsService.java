package com.pulsequeue.aws;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.cloudwatch.CloudWatchClient;
import software.amazon.awssdk.services.cloudwatch.model.*;

import java.time.Instant;
import java.util.List;

@Service
@Slf4j
public class CloudWatchMetricsService {

    private final CloudWatchClient cloudWatchClient;

    @Value("${pulsequeue.aws.cloudwatch.enabled:false}")
    private boolean enabled;

    private static final String NAMESPACE = "PulseQueue";

    public CloudWatchMetricsService(
            @Value("${pulsequeue.aws.access-key-id:#{null}}") String accessKeyId,
            @Value("${pulsequeue.aws.secret-access-key:#{null}}") String secretAccessKey,
            @Value("${pulsequeue.aws.region:us-east-1}") String region) {

        if (accessKeyId != null && secretAccessKey != null) {
            this.cloudWatchClient = CloudWatchClient.builder()
                .region(Region.of(region))
                .credentialsProvider(StaticCredentialsProvider.create(
                    AwsBasicCredentials.create(accessKeyId, secretAccessKey)
                ))
                .build();
            log.info("CloudWatch client initialized for region={}", region);
        } else {
            this.cloudWatchClient = null;
            log.warn("AWS credentials not configured - CloudWatch metrics disabled");
        }
    }

    public void putEventsIngested(int count, String serviceId) {
        putMetric("EventsIngested", count, StandardUnit.COUNT,
            Dimension.builder().name("ServiceId").value(serviceId).build());
    }

    public void putKafkaProduceLatency(long latencyMs) {
        putMetric("KafkaProduceLatency", latencyMs, StandardUnit.MILLISECONDS);
    }

    public void putDlqMessageCount(int count) {
        putMetric("DLQMessageCount", count, StandardUnit.COUNT);
    }

    public void putActiveServices(int count) {
        putMetric("ActiveServices", count, StandardUnit.COUNT);
    }

    private void putMetric(String metricName, double value, StandardUnit unit, Dimension... dims) {
        if (!enabled || cloudWatchClient == null) return;
        try {
            MetricDatum.Builder datum = MetricDatum.builder()
                .metricName(metricName)
                .value(value)
                .unit(unit)
                .timestamp(Instant.now());

            if (dims.length > 0) {
                datum.dimensions(List.of(dims));
            }

            cloudWatchClient.putMetricData(PutMetricDataRequest.builder()
                .namespace(NAMESPACE)
                .metricData(datum.build())
                .build());

            log.debug("CloudWatch metric published: {}={} {}", metricName, value, unit);
        } catch (Exception e) {
            log.warn("CloudWatch metric publish failed: {}", e.getMessage());
        }
    }
}
