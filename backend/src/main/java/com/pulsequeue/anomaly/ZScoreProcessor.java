package com.pulsequeue.anomaly;

import com.pulsequeue.model.MetricAggregate;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import org.apache.kafka.streams.state.KeyValueStore;

@Slf4j
public class ZScoreProcessor implements FixedKeyProcessor<String, MetricAggregate, MetricAggregate> {

    public static final String STORE_NAME = "anomaly-state-store";
    static final double THRESHOLD = 2.5;

    private FixedKeyProcessorContext<String, MetricAggregate> context;
    private KeyValueStore<String, AnomalyState> stateStore;

    @Override
    public void init(FixedKeyProcessorContext<String, MetricAggregate> context) {
        this.context = context;
        this.stateStore = context.getStateStore(STORE_NAME);
    }

    @Override
    public void process(FixedKeyRecord<String, MetricAggregate> record) {
        String serviceId = record.key();
        MetricAggregate aggregate = record.value();

        if (aggregate == null) {
            context.forward(record);
            return;
        }

        AnomalyState state = stateStore.get(serviceId);
        if (state == null) {
            state = new AnomalyState();
        }

        double zP99 = state.zScoreP99(aggregate.getP99());
        double zErr = state.zScoreErrorRate(aggregate.getErrorRate());

        // Update state AFTER scoring so the current window doesn't inflate its own z-score
        state.update(aggregate.getP99(), aggregate.getErrorRate());
        stateStore.put(serviceId, state);

        aggregate.setZScoreP99(zP99);
        aggregate.setZScoreErrorRate(zErr);

        boolean anomaly = Math.abs(zP99) > THRESHOLD || Math.abs(zErr) > THRESHOLD;
        aggregate.setAnomalyDetected(anomaly);

        if (anomaly) {
            String reason = buildReason(zP99, zErr);
            aggregate.setAnomalyReason(reason);
            log.warn("ANOMALY: service={} zP99={} zErr={} n={} reason={}",
                serviceId,
                String.format("%.2f", zP99),
                String.format("%.2f", zErr),
                state.getN(),
                reason);
        }

        context.forward(record.withValue(aggregate));
    }

    @Override
    public void close() {}

    private String buildReason(double zP99, double zErr) {
        boolean p99Anomalous = Math.abs(zP99) > THRESHOLD;
        boolean errAnomalous = Math.abs(zErr) > THRESHOLD;

        if (p99Anomalous && errAnomalous) {
            return String.format("p99 z=%.2f and error_rate z=%.2f both exceed threshold %.1f", zP99, zErr, THRESHOLD);
        } else if (p99Anomalous) {
            return String.format("p99 latency z-score=%.2f exceeds threshold %.1f", zP99, THRESHOLD);
        } else {
            return String.format("error_rate z-score=%.2f exceeds threshold %.1f", zErr, THRESHOLD);
        }
    }
}
