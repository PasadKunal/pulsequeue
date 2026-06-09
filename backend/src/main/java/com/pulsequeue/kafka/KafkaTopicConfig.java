package com.pulsequeue.kafka;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicConfig {

    @Value("${pulsequeue.kafka.topics.events-raw}")
    private String eventsRawTopic;

    @Value("${pulsequeue.kafka.topics.metrics-aggregated}")
    private String metricsAggregatedTopic;

    @Value("${pulsequeue.kafka.topics.anomalies}")
    private String anomaliesTopic;

    @Bean
    public NewTopic eventsRawTopic() {
        return TopicBuilder.name(eventsRawTopic)
            .partitions(3)
            .replicas(1)
            .build();
    }

    @Bean
    public NewTopic metricsAggregatedTopic() {
        return TopicBuilder.name(metricsAggregatedTopic)
            .partitions(1)
            .replicas(1)
            .build();
    }

    @Bean
    public NewTopic anomaliesTopic() {
        return TopicBuilder.name(anomaliesTopic)
            .partitions(1)
            .replicas(3)
            .build();
    }
}
