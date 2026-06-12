# PulseQueue

![CI](https://github.com/PasadKunal/pulsequeue/actions/workflows/ci.yml/badge.svg)

**Live demo:** [pulsequeue-mu.vercel.app](https://pulsequeue-mu.vercel.app) &nbsp;|&nbsp; **API health:** [pulsequeue-f1e3.onrender.com/actuator/health](https://pulsequeue-f1e3.onrender.com/actuator/health)

PulseQueue is a distributed event streaming and observability platform. Services send raw HTTP telemetry to a REST endpoint, Kafka Streams aggregates it into 1-minute windows to compute p50/p95/p99 latency and error rates, a Welford online algorithm flags statistical anomalies without storing history, and everything is pushed live to a React dashboard over SSE.

---

## Architecture

```
POST /v1/events
       |
  +-----------------+
  | Spring Boot API |  batch validation, timestamp enrichment
  +--------+--------+
           | produce (keyed by sourceId)
           v
  +------------------+
  |  Redpanda Cloud  |  events.raw (3 partitions)
  +--------+---------+
           |
           v
  +--------------------------------------------+
  |              Kafka Streams                 |
  |  - 1-min tumbling windows (10s grace)      |
  |  - p50 / p95 / p99 via percentile sort     |
  |  - Welford online z-score (p99, errRate)   |
  +----------+-------------------------+--------+
             |                         | anomalyDetected=true
    metrics.aggregated          metrics.anomalies
             |                         |
  +----------+---------+    +----------+----------+
  |   MetricsConsumer  |    |  AnomalyConsumer    +---> Slack alert
  +----------+---------+    +---------------------+
             |
  +----------+-------------+
  |  TimescaleDB (Neon)    |  hypertable partitioned by window_start
  +----------+-------------+
             |
  +----------+------+    +----------------------------------+
  |  Redis Cache    |    |  AWS side-channels               |
  |  (Upstash)      |    |  S3  - hourly GZip -> Glacier 90d|
  +----------+------+    |  SQS - DLQ + recovery worker     |
             |           |  CloudWatch - custom metrics      |
             |           +----------------------------------+
             | SSE every 5s
  +----------+------+
  |  React / Vite   |  Recharts p50/p95/p99 line charts, auto-reconnect
  +-----------------+
```

---

## Features

- **Event ingestion:** batch POST endpoint with validation, timestamp normalization, and idempotent Kafka produce
- **Stream processing:** Kafka Streams 1-minute tumbling windows backed by a RocksDB state store; p50/p95/p99 computed from sorted latency arrays per window
- **Anomaly detection:** Welford's online algorithm maintains a running mean and variance per service with no stored history; z-scores above 2.5 sigma on p99 or error rate trigger an alert
- **Threshold alerting:** a scheduled poller checks DB-backed alert rules every 30 seconds and fires Slack webhooks when a threshold is breached
- **Live dashboard:** an SSE endpoint pushes aggregated metrics every 5 seconds to a React frontend that reconnects automatically on disconnect
- **Cold archival:** an hourly job writes GZip-compressed JSON snapshots to S3, with a Glacier lifecycle policy kicking in after 90 days
- **Dead-letter queue:** failed Kafka produce events go to an SQS DLQ and a recovery worker retries them every 60 seconds
- **Observability:** CloudWatch custom metrics for `EventsIngested` and `KafkaProduceLatency` per service
- **Client SDK:** a Spring Boot starter that auto-instruments any Spring MVC service with a servlet filter
- **Load tested:** Gatling 5-phase simulation (ramp, steady, peak, spike, cooldown) with p95 = 148ms across 8,660 requests and zero failures

---

## Tech Stack

| Layer | Technology |
|---|---|
| Backend | Java 21, Spring Boot 3.2, Spring Kafka |
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
| Client SDK | Spring Boot starter (auto-instruments any Spring MVC service) |

---

## SDK

Any Spring Boot (MVC) service can report to PulseQueue with no boilerplate code. A servlet filter intercepts every request, measures latency, and batches events asynchronously. The host service is never blocked, and if PulseQueue is unreachable, events are quietly dropped.

**1. Add the JitPack repository and dependency to your `pom.xml`:**

```xml
<repositories>
    <repository>
        <id>jitpack.io</id>
        <url>https://jitpack.io</url>
    </repository>
</repositories>

<dependency>
    <groupId>com.github.PasadKunal.pulsequeue</groupId>
    <artifactId>pulsequeue-spring-boot-starter</artifactId>
    <version>v1.0.0</version>
</dependency>
```

**2. Add two lines to your `application.yml`:**

```yaml
pulsequeue:
  endpoint: https://pulsequeue-f1e3.onrender.com
  service-name: payment-svc
```

That's all. Every HTTP request your service handles will automatically appear on the dashboard.

**All configuration options:**

```yaml
pulsequeue:
  endpoint: https://pulsequeue-f1e3.onrender.com
  service-name: payment-svc
  api-key: your-key                  # optional, sent as X-Api-Key header
  batch-size: 50                     # flush after this many events (default 50)
  flush-interval-seconds: 5          # flush every N seconds (default 5)
  exclude-paths:                     # paths to skip, prefix-matched
    - /actuator
    - /health
  enabled: false                     # set to false to disable without removing the dependency
```

---

## Try it yourself

The repo includes a demo Spring Boot service already wired up with the SDK. Run it locally to see events flowing into the live dashboard.

**1. Install the SDK to your local Maven repo:**

```bash
cd sdk && mvn install -DskipTests -q
```

**2. Start the demo service:**

```bash
cd demo && mvn spring-boot:run
```

The demo points at the hosted Render backend, so you don't need to run the backend locally.

**3. Hit the demo endpoints:**

```bash
curl http://localhost:9090/fast    # 10-50ms response
curl http://localhost:9090/slow    # 400-700ms response
curl http://localhost:9090/error   # returns HTTP 500
curl http://localhost:9090/spike   # 2000-3000ms, good for triggering anomaly detection
```

Send a batch of requests, wait about 75 seconds for the Kafka Streams window to close, then send another batch. `demo-svc` will appear on the [live dashboard](https://pulsequeue-mu.vercel.app) with real latency charts.

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

### 4. Send a test event

```bash
curl -X POST http://localhost:8080/v1/events \
  -H "Content-Type: application/json" \
  -d '[
    {"sourceId":"payment-svc","latencyMs":150,"statusCode":200},
    {"sourceId":"auth-svc","latencyMs":40,"statusCode":200},
    {"sourceId":"payment-svc","latencyMs":950,"statusCode":500}
  ]'
```

Send another batch after 75 seconds to advance the Kafka Streams watermark past the window grace period. Data appears on the dashboard within a few seconds of the first window closing.

### 5. Run the load test

```bash
cd gatling
mvn gatling:test
```

Results land in `gatling/target/gatling/pulsequeue-*/index.html`.

---

## API

| Method | Path | Description |
|---|---|---|
| `POST` | `/v1/events` | Ingest a batch of events (max 500 per request) |
| `GET` | `/v1/services` | List active services from the last 24 hours |
| `GET` | `/v1/metrics?service=<id>&range=6h` | Metric history for a service |
| `GET` | `/v1/dashboard/stream` | SSE stream pushing `metrics` events every 5 seconds |
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

## Deployment

### Backend (Render)

1. New Web Service, connect `PasadKunal/pulsequeue`, set root directory to `backend`
2. Runtime: **Docker** (picks up `backend/Dockerfile` automatically)
3. Add environment variables for all secrets (mirror the keys from `application-local.yml` using the `SPRING_` prefix for Spring properties), plus `SPRING_PROFILES_ACTIVE=prod`
4. Deploy. Kafka Streams takes about 60 seconds to reach RUNNING state. Health check is at `/actuator/health`

### Frontend (Vercel)

1. New Project, import `PasadKunal/pulsequeue`, set root directory to `frontend`
2. Framework: **Vite** (auto-detected)
3. Add environment variable: `VITE_API_URL=https://pulsequeue-f1e3.onrender.com`
4. Deploy. Builds in about 30 seconds and auto-deploys on every push to `main`

---

## CI

GitHub Actions runs on every push to `main`:

1. Build backend JAR (`mvn package -DskipTests`)
2. Compile Gatling simulations (`mvn test-compile`)
3. Build Docker image
4. Upload JAR as a build artifact
