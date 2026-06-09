package com.pulsequeue.anomaly;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Welford's online algorithm — maintains running mean and variance per service
 * without storing all historical samples. Used by ZScoreProcessor in Kafka Streams.
 */
@Data
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class AnomalyState {

    private long n = 0;
    private double meanP99 = 0.0;
    private double m2P99 = 0.0;
    private double meanErrorRate = 0.0;
    private double m2ErrorRate = 0.0;

    public void update(double p99, double errorRate) {
        n++;

        double deltaP99 = p99 - meanP99;
        meanP99 += deltaP99 / n;
        m2P99 += deltaP99 * (p99 - meanP99);

        double deltaErr = errorRate - meanErrorRate;
        meanErrorRate += deltaErr / n;
        m2ErrorRate += deltaErr * (errorRate - meanErrorRate);
    }

    public double zScoreP99(double value) {
        if (n < 10) return 0.0;
        double stddev = Math.sqrt(m2P99 / (n - 1));
        return stddev < 0.001 ? 0.0 : (value - meanP99) / stddev;
    }

    public double zScoreErrorRate(double value) {
        if (n < 10) return 0.0;
        double stddev = Math.sqrt(m2ErrorRate / (n - 1));
        return stddev < 0.0001 ? 0.0 : (value - meanErrorRate) / stddev;
    }
}
