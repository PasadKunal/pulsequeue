# PulseQueue — System Design

## Problem Statement

Given a fleet of microservices emitting latency and status events at high throughput, provide:
1. Real-time per-service latency percentiles (p50/p95/p99) and error rates
2. Automatic detection of statistical anomalies without manual threshold tuning
3. Durable storage for both hot queries (last 24h) and long-term archival
4. A live dashboard with sub-10-second data freshness

---

## High-Level Architecture

```
Producers
   │  POST /v1/events (batch)
   ▼
┌──────────────────────────────────────────────────────────────────┐
│                        Spring Boot API                           │
│  • Validates and enriches events (timestamp normalization)       │
│  • Publishes to Kafka keyed by sourceId                          │
│  • Async: CloudWatch metrics, SQS DLQ on produce failure         │
└───────────────────────────────┬──────────────────────────────────┘
                                │
                         events.raw (Redpanda, 3 partitions)
                                │
┌───────────────────────────────▼──────────────────────────────────┐
│                      Kafka Streams                               │
│  groupByKey → windowedBy(1 min, 10s grace) → aggregate          │
│  → processValues(ZScoreProcessor)                                │
│                                                                  │
│  Output fan-out:                                                 │
│    metrics.aggregated  → all windows                             │
│    metrics.anomalies   → windows where z-score > 2.5σ           │
└────────────────────┬───────────────────────┬─────────────────────┘
                     │                       │
             MetricsConsumer         AnomalyAlertConsumer
                     │                       │
              TimescaleDB              Slack webhook
              (Neon + JPA)
                     │
               Redis cache (Upstash)
                     │
        GET /v1/dashboard/stream (SSE)
                     │
              React + Recharts
```

---

## Component Design

### Event Ingestion (`EventIngestionService`)

Events are batched at the HTTP layer. Each event carries `sourceId`, `latencyMs`, `statusCode`, and an optional `timestamp` (backfilled to `Instant.now()` if absent). Kafka messages are produced with `sourceId` as the key, which ensures all events for a service land on the same partition and are processed by the same Kafka Streams task.

**SQS DLQ**: if the Kafka producer callback receives an exception, the event is published to an SQS dead-letter queue. A `@Scheduled` recovery worker polls the DLQ every 60 seconds and retries failed events.

### Stream Processing (`MetricsStreamProcessor`)

The Kafka Streams topology has two stages:

**Stage 1 — Windowed aggregation**

```
stream(events.raw)
  .groupByKey()
  .windowedBy(TimeWindows.ofSizeAndGrace(1 min, 10 s))
  .aggregate(MetricAggregate::new, accumulator, Materialized.as("metrics-store"))
  .toStream()
  .selectKey(windowedKey → windowedKey.key())
```

`MetricAggregate` accumulates a list of latency values and an error count. After the window closes, `computePercentiles()` sorts the latency list and reads the p50/p95/p99 positions directly. This is O(n log n) per window closure but bounded by the number of events in one minute per partition — typically well under 10,000 in this workload.

**Stage 2 — Z-score enrichment**

```
.processValues(ZScoreProcessor::new, STORE_NAME)
```

`ZScoreProcessor` reads per-service `AnomalyState` from a RocksDB key-value store (logging disabled to avoid Redpanda Cloud's `message.timestamp.type` restriction), computes z-scores against the running distribution, updates the state, then forwards the enriched record. Anomalous windows also fan-out to `metrics.anomalies`.

### Anomaly Detection (`ZScoreProcessor` + `AnomalyState`)

Welford's online algorithm maintains `(n, mean, M2)` per metric without storing the full history:

```
delta  = x − mean
mean  += delta / n
M2    += delta × (x − mean)          // uses updated mean
stddev = sqrt(M2 / (n − 1))
z      = (x − mean) / stddev
```

Computing z-scores **before** updating state ensures the current window does not inflate its own score. Z-scores are suppressed (returned as 0.0) during a warmup period of n < 10 windows to avoid false positives on startup.

Two independent z-scores are computed per window: one for p99 latency and one for error rate. Either exceeding 2.5σ triggers an anomaly flag.

### Persistence (`MetricsPersistenceService`)

`MetricsConsumer` listens on `metrics.aggregated` and persists each window to TimescaleDB via JPA. The `metric_aggregates` table is a TimescaleDB hypertable partitioned by `window_start` with a 7-day chunk interval. This keeps hot chunks in memory while older chunks are compressed automatically.

After each write, `MetricsCacheService` writes the latest aggregate to Redis with a TTL matching the window size (60 seconds). Cache misses fall back to a direct DB query. Redis failures are caught and logged — the system degrades gracefully to cache-miss-only reads.

### Cold Archival (`S3ArchivalService`)

`@Scheduled` at the top of every hour, the service:
1. Queries all metric aggregates from the previous hour
2. Serializes to JSON and compresses with GZip
3. Uploads to S3 at `metrics/<year>/<month>/<day>/<hour>.json.gz`

An S3 Lifecycle policy transitions objects to Glacier after 90 days, reducing storage cost for long-term retention.

### Live Dashboard

`StreamController` exposes `GET /v1/dashboard/stream` as a Spring WebFlux `Flux<ServerSentEvent>`. On each 5-second tick it queries active services and their 6-hour history from TimescaleDB (or Redis for the latest window), then pushes a `metrics` SSE event. The React frontend reconnects automatically on disconnect with a 3-second backoff.

---

## Data Flow Timing

```
t=0      Event POSTed, produced to Kafka
t~0      Kafka Streams receives event, accumulates into open window
t=60s    Window closes (grace period: +10s)
t=70s    Next event advances stream time past window end + grace
t~71s    Window aggregate emitted, z-scores computed
t~72s    MetricsConsumer persists to TimescaleDB
t~77s    SSE tick pushes updated data to frontend
t~77s    React dashboard renders new data point
```

End-to-end latency from event ingestion to dashboard: **~77 seconds** in the nominal case (dominated by the 1-minute window size + 10-second grace period).

---

## Scalability

| Bottleneck | Current | Scale-out path |
|---|---|---|
| Kafka partitions | 3 (events.raw) | Increase partitions; Kafka Streams scales tasks 1:1 with partitions |
| Streams threads | 1 | Set `num.stream.threads` to match partition count |
| TimescaleDB | Single Neon instance | Read replicas for dashboard queries; chunk compression for older data |
| Redis | Single Upstash instance | Redis Cluster for horizontal write distribution |
| SSE fanout | Flux per connection | Add a Pub/Sub broker (e.g. Redis pub/sub) to decouple DB polling from connections |
| API ingestion | Single Spring Boot instance | Stateless — horizontal scaling behind a load balancer |

---

## Key Design Decisions

**Why Welford's algorithm instead of a sliding window of raw values?**  
Storing raw historical values would grow unbounded and require a periodic flush strategy. Welford's runs in O(1) space and O(1) time per update regardless of history length. The tradeoff is that the distribution estimate assumes stationarity — a service that permanently shifts its baseline will eventually adapt (mean tracks the new level) rather than alert indefinitely.

**Why event-time windowing with a grace period?**  
Kafka Streams uses record timestamps (event time) rather than wall-clock time. This correctly handles late-arriving events (network jitter, batch retries) by holding the window open for 10 seconds past its nominal end. Without a grace period, events arriving slightly late would be silently dropped from their intended window.

**Why `withLoggingDisabled()` on the anomaly state store?**  
Redpanda Cloud rejects topic creation requests that include `message.timestamp.type`, which Kafka Streams sets unconditionally on changelog topics. Disabling logging removes the changelog entirely. For anomaly detection state (running statistics), this is acceptable — on restart the warmup period repeats and z-scores return to 0.0 for the first 10 windows.

**Why TimescaleDB over plain PostgreSQL?**  
The `metric_aggregates` workload is an append-only time series with frequent range-scan queries (`WHERE window_start > ?`). TimescaleDB's automatic chunk partitioning keeps each chunk small and in-memory, giving index scans comparable to specialized time-series databases while retaining full SQL semantics for alert rule evaluation.

**Why SSE instead of WebSocket?**  
The dashboard is read-only — the server pushes, clients consume. SSE is simpler than WebSocket (no upgrade handshake, standard `EventSource` API in the browser, automatic reconnect built-in), works through HTTP/2 multiplexing, and requires no additional protocol layer.
