package com.pulsequeue.kafka;

import com.pulsequeue.model.IngestEvent;
import com.pulsequeue.model.MetricAggregate;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.WindowStore;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.time.Duration;

@Configuration
@EnableKafkaStreams
@Slf4j
public class MetricsStreamProcessor {

    @Value("${pulsequeue.kafka.topics.events-raw}")
    private String eventsRawTopic;

    @Value("${pulsequeue.kafka.topics.metrics-aggregated}")
    private String metricsAggregatedTopic;

    @Bean
    public KStream<String, IngestEvent> metricsStream(StreamsBuilder streamsBuilder) {

        JsonSerde<IngestEvent> eventSerde = new JsonSerde<>(IngestEvent.class);
        JsonSerde<MetricAggregate> aggregateSerde = new JsonSerde<>(MetricAggregate.class);

        KStream<String, IngestEvent> eventStream = streamsBuilder
            .stream(eventsRawTopic, Consumed.with(Serdes.String(), eventSerde));

        eventStream
            .groupByKey()
            .windowedBy(
                TimeWindows.ofSizeAndGrace(Duration.ofMinutes(1), Duration.ofSeconds(10))
            )
            .aggregate(
                MetricAggregate::new,
                (serviceId, event, aggregate) -> {
                    aggregate.setServiceId(serviceId);
                    aggregate.addEvent(event);
                    return aggregate;
                },
                Materialized.<String, MetricAggregate, WindowStore<org.apache.kafka.common.utils.Bytes, byte[]>>as("metrics-store")
                    .withKeySerde(Serdes.String())
                    .withValueSerde(aggregateSerde)
            )
            .toStream()
            .peek((windowedKey, aggregate) -> {
                if (aggregate != null) {
                    aggregate.setWindowStart(
                        java.time.Instant.ofEpochMilli(windowedKey.window().startTime().toEpochMilli())
                    );
                    aggregate.setWindowEnd(
                        java.time.Instant.ofEpochMilli(windowedKey.window().endTime().toEpochMilli())
                    );
                    aggregate.computePercentiles();
                    log.info("Window aggregate: service={} events={} p50={}ms p95={}ms p99={}ms errorRate={}",
                        aggregate.getServiceId(),
                        aggregate.getTotalEvents(),
                        aggregate.getP50(),
                        aggregate.getP95(),
                        aggregate.getP99(),
                        String.format("%.2f", aggregate.getErrorRate())
                    );
                }
            })
            .selectKey((windowedKey, aggregate) -> windowedKey.key())
            .to(metricsAggregatedTopic, Produced.with(Serdes.String(), aggregateSerde));

        return eventStream;
    }
}
