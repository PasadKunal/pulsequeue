# PulseQueue

![CI](https://github.com/kunalpasad/pulsequeue/actions/workflows/ci.yml/badge.svg)

A distributed event streaming and observability platform. Services POST raw events; Kafka Streams aggregates them into 1-minute windows computing p50/p95/p99 latency and error rate per service; a Welford online algorithm detects statistical anomalies in real time; results are persisted to TimescaleDB and surfaced on a live React dashboard via SSE.

---

## Architecture

```
POST /v1/events
      │
┌─────▼────────────┐
│  Spring Boot API  │  batch validation · timestamp enrichment
└─────┬─────────────┘
      │ produce (keyed by sourceId)
      ▼
┌─────────────────┐
│  Redpanda Cloud  │  events.raw  (3 partitions)
└─────┬───────────┘
      │
      ▼
┌──────────────────────────────────────────┐
│           Kafka Streams                  │
│  • 1-min tumbling windows (10s grace)    │
│  • p50 / p95 / p99 via percentile sort   │
│  • Welford online z-score (p99, errRate) │
└───────┬───────────────────┬──────────────┘
        │                   │ anomalyDetected=true
  metrics.aggregated   metrics.anomalies
        │                   │
┌───────▼──────┐    ┌───────▼──────┐
│MetricsConsumer│    │AnomalyConsumer│──► Slack alert
└───────┬──────┘    └──────────────┘
        │
┌───────▼──────────────┐
│  TimescaleDB (Neon)  │  hypertable partitioned by window_start
└───────┬──────────────┘
        │
┌───────▼──────┐    ┌─────────────────────────────────┐
│ Redis Cache  │    │  AWS side-channels               │
│ (Upstash)    │    │  S3  — hourly GZip → Glacier 90d │
└───────┬──────┘    │  SQS — DLQ + recovery worker     │
        │           │  CloudWatch — custom metrics      │
        │           └─────────────────────────────────┘
        │ SSE every 5s
┌───────▼──────┐
│ React/Vite   │  Recharts p50/p95/p99 line charts · auto-reconnect
└──────────────┘
```

---

## Features

- **Event ingestion** — batch POST endpoint with validation, timestamp normalization, and idempotent Kafka produce
- **Stream processing** — Kafka Streams 1-minute tumbling windows with RocksDB state store; p50/p95/p99 computed from sorted latency arrays
- **Anomaly detection** — Welford's online algorithm tracks running mean/variance per service without storing history; z-scores > 2.5σ on p99 or error rate trigger alerts
- **Threshold alerting** — `@Scheduled` poller evaluates DB-backed alert rules every 30 seconds, fires Slack webhooks on breach
- **Live dashboard** — Spring WebFlux SSE endpoint pushes aggregated metrics every 5 seconds to a React frontend with auto-reconnect
- **Cold archival** — S3ArchivalService runs hourly, writes GZip-compressed JSON snapshots, Glacier lifecycle policy after 90 days
- **Dead-letter queue** — failed Kafka produce events routed to SQS DLQ; `DlqRecoveryWorker` retries on a 60-second schedule
- **Observability** — CloudWatch custom metrics: `EventsIngested` and `KafkaProduceLatency` per service
- **Load tested** — Gatling 5-phase simulation (ramp → steady → peak → spike → cooldown); p95=148ms, 8,660 requests, 0 failures

---

## Tech Stack

| Layer | Technology |
|---|---|
| Backend | Java 21, Spring Boot 3.2, Spring Kafka, Spring WebFlux |
| Stream processing | Kafka Streams 3.6, RocksDB state store |
| Message broker | Redpanda Cloud (Kafka-compatible) |
| Hot storage | TimescaleDB on Neon PostgreSQL |
| Cold storage | AWS S3 + Glacier lifecycle |
| Cache | Upstash Redis |
| Dead-letter queue | AWS SQS |
| Observability | AWS CloudWatch |
| Frontend | React 18, Vite, Recharts |
| Load testing | Gatling 3.9 |
| CI/CD | GitHub Actions + Docker multi-stage build |

---

## Local Setup

### Prerequisites

- Java 21
- Maven 3.9+
- Node 18+
- Accounts: Redpanda Cloud, Neon PostgreSQL, Upstash Redis, AWS

### 1. Configure secrets

Create `backend/src/main/resources/application-local.yml` (gitignored):

```yaml
spring:
  datasource:
    url: jdbc:postgresql://<neon-host>/neondb?sslmode=require
    username: <user>
    password: <password>
  redis:
    host: <upstash-host>
    port: 6379
    password: <upstash-password>
    ssl.enabled: true
  kafka:
    bootstrap-servers: <redpanda-broker>:9092
    properties:
      security.protocol: SASL_SSL
      sasl.mechanism: SCRAM-SHA-256
      sasl.jaas.config: >
        org.apache.kafka.common.security.scram.ScramLoginModule required
        username="<user>" password="<password>";
    streams:
      replication-factor: 3
      properties:
        security.protocol: SASL_SSL
        sasl.mechanism: SCRAM-SHA-256
        sasl.jaas.config: >
          org.apache.kafka.common.security.scram.ScramLoginModule required
          username="<user>" password="<password>";
        replication.factor: 3

pulsequeue:
  alerting:
    slack-webhook-url: https://hooks.slack.com/services/...
  aws:
    access-key-id: <key>
    secret-access-key: <secret>
    region: us-east-2
    s3.bucket: <bucket>
    sqs.dlq-url: https://sqs.us-east-2.amazonaws.com/<account>/<queue>
```

### 2. Start the backend

```bash
cd backend
mvn spring-boot:run -Dspring-boot.run.profiles=local
```

### 3. Start the frontend

```bash
cd frontend
npm install
npm run dev
```

Open [http://localhost:5173](http://localhost:5173).

### 4. Send events

```bash
curl -X POST http://localhost:8080/v1/events \
  -H "Content-Type: application/json" \
  -d '[
    {"sourceId":"payment-svc","latencyMs":150,"statusCode":200},
    {"sourceId":"auth-svc","latencyMs":40,"statusCode":200},
    {"sourceId":"payment-svc","latencyMs":950,"statusCode":500}
  ]'
```

Wait ~70 seconds, then send again to advance the Kafka Streams watermark past the window grace period. Data appears on the dashboard within seconds of the first window emitting.

### 5. Run the load test

With the backend running:

```bash
cd gatling
mvn gatling:test
```

Report: `gatling/target/gatling/pulsequeue-*/index.html`

---

## API

| Method | Path | Description |
|---|---|---|
| `POST` | `/v1/events` | Ingest a batch of events |
| `GET` | `/v1/services` | List active services (last 24h) |
| `GET` | `/v1/metrics?service=<id>&range=6h` | Metric history for a service |
| `GET` | `/v1/dashboard/stream` | SSE stream — emits `metrics` events every 5s |
| `GET` | `/actuator/health` | Health check |

---

## Docker

```bash
cd backend
docker build -t pulsequeue .
docker run -p 8080:8080 \
  -e SPRING_PROFILES_ACTIVE=local \
  -v $(pwd)/src/main/resources/application-local.yml:/app/config/application-local.yml \
  pulsequeue
```

---

## CI

GitHub Actions runs on every push to `main`:
1. Build backend JAR (`mvn package -DskipTests`)
2. Compile Gatling simulations (`mvn test-compile`)
3. Build Docker image
4. Upload JAR as build artifact
