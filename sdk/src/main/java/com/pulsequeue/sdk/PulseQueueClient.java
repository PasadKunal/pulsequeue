package com.pulsequeue.sdk;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

public class PulseQueueClient implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(PulseQueueClient.class);

    private final PulseQueueProperties props;
    private final ObjectMapper objectMapper;
    private final HttpClient httpClient;
    private final BlockingQueue<PulseQueueEvent> queue;
    private final ScheduledExecutorService scheduler;

    public PulseQueueClient(PulseQueueProperties props, ObjectMapper objectMapper) {
        this.props = props;
        this.objectMapper = objectMapper;
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(5))
                .build();
        // Bounded queue - drops events silently if the flush thread falls behind
        this.queue = new LinkedBlockingQueue<>(10_000);
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "pulsequeue-flush");
            t.setDaemon(true);
            return t;
        });
        this.scheduler.scheduleAtFixedRate(
                this::flush,
                props.getFlushIntervalSeconds(),
                props.getFlushIntervalSeconds(),
                TimeUnit.SECONDS
        );
    }

    public void record(String sourceId, long latencyMs, int statusCode) {
        boolean accepted = queue.offer(PulseQueueEvent.of(sourceId, latencyMs, statusCode));
        if (!accepted) {
            log.debug("PulseQueue buffer full, dropping event for {}", sourceId);
            return;
        }
        // Flush early if batch is ready
        if (queue.size() >= props.getBatchSize()) {
            scheduler.execute(this::flush);
        }
    }

    private void flush() {
        if (queue.isEmpty()) return;

        List<PulseQueueEvent> batch = new ArrayList<>(props.getBatchSize());
        queue.drainTo(batch, props.getBatchSize());
        if (batch.isEmpty()) return;

        try {
            String body = objectMapper.writeValueAsString(batch);
            HttpRequest.Builder requestBuilder = HttpRequest.newBuilder()
                    .uri(URI.create(props.getEndpoint() + "/v1/events"))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(body))
                    .timeout(Duration.ofSeconds(10));

            if (props.getApiKey() != null && !props.getApiKey().isBlank()) {
                requestBuilder.header("X-Api-Key", props.getApiKey());
            }

            HttpResponse<Void> response = httpClient.send(
                    requestBuilder.build(),
                    HttpResponse.BodyHandlers.discarding()
            );

            if (response.statusCode() >= 400) {
                log.warn("PulseQueue rejected batch of {} events (HTTP {})", batch.size(), response.statusCode());
            } else {
                log.debug("Flushed {} events to PulseQueue", batch.size());
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            // Never crash the host service - just log at debug level
            log.debug("Could not flush {} events to PulseQueue: {}", batch.size(), e.getMessage());
        }
    }

    @Override
    public void close() {
        scheduler.shutdown();
        flush();
    }
}
