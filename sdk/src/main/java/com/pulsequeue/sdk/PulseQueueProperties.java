package com.pulsequeue.sdk;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.ArrayList;
import java.util.List;

@ConfigurationProperties(prefix = "pulsequeue")
public class PulseQueueProperties {

    /** Whether the SDK is active. Set to false to disable without removing the dependency. */
    private boolean enabled = true;

    /** Full base URL of the PulseQueue API, e.g. https://pulsequeue-f1e3.onrender.com */
    private String endpoint;

    /** Name reported as sourceId in every event. Defaults to spring.application.name. */
    private String serviceName;

    /** Optional API key sent as X-Api-Key header. */
    private String apiKey;

    /** Max events per HTTP batch. Flush triggers early if this is reached before the interval. */
    private int batchSize = 50;

    /** How often to flush buffered events (seconds). */
    private long flushIntervalSeconds = 5;

    /** Request paths to skip. Prefix-matched. */
    private List<String> excludePaths = new ArrayList<>(List.of("/actuator", "/health", "/favicon.ico"));

    public boolean isEnabled() { return enabled; }
    public void setEnabled(boolean enabled) { this.enabled = enabled; }

    public String getEndpoint() { return endpoint; }
    public void setEndpoint(String endpoint) { this.endpoint = endpoint; }

    public String getServiceName() { return serviceName; }
    public void setServiceName(String serviceName) { this.serviceName = serviceName; }

    public String getApiKey() { return apiKey; }
    public void setApiKey(String apiKey) { this.apiKey = apiKey; }

    public int getBatchSize() { return batchSize; }
    public void setBatchSize(int batchSize) { this.batchSize = batchSize; }

    public long getFlushIntervalSeconds() { return flushIntervalSeconds; }
    public void setFlushIntervalSeconds(long flushIntervalSeconds) { this.flushIntervalSeconds = flushIntervalSeconds; }

    public List<String> getExcludePaths() { return excludePaths; }
    public void setExcludePaths(List<String> excludePaths) { this.excludePaths = excludePaths; }
}
