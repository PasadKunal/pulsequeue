package com.pulsequeue.sdk;

import java.time.Instant;

public record PulseQueueEvent(
        String sourceId,
        long latencyMs,
        int statusCode,
        String timestamp
) {
    public static PulseQueueEvent of(String sourceId, long latencyMs, int statusCode) {
        return new PulseQueueEvent(sourceId, latencyMs, statusCode, Instant.now().toString());
    }
}
