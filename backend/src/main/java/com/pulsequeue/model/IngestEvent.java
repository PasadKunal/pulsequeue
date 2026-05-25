package com.pulsequeue.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class IngestEvent {

    private String sourceId;
    private String eventType;
    private Long latencyMs;
    private Integer statusCode;
    private Instant timestamp;

    public boolean isError() {
        return statusCode != null && statusCode >= 500;
    }

    public void enrichTimestamp() {
        if (this.timestamp == null) {
            this.timestamp = Instant.now();
        }
    }
}
