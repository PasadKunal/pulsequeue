# PulseQueue

A distributed event streaming and observability platform built with Java, Spring Boot, Kafka Streams, and AWS.

## What it does

PulseQueue ingests high-throughput structured events (logs, metrics, traces) from multiple producers, processes them through a Kafka Streams processor computing real-time p50/p95/p99 latency aggregations, persists to TimescaleDB (hot) and AWS S3 (cold), and surfaces live dashboards with anomaly-based alerting.

## Architecture

> Architecture diagram coming in a later commit.

## Tech stack

| Layer | Technology |
|---|---|
| Backend | Java 21, Spring Boot 3, Spring Kafka, Spring WebFlux |
| Stream processing | Kafka Streams, RocksDB state store |
| Message broker | Upstash Kafka |
| Hot storage | TimescaleDB on Neon PostgreSQL |
| Cold storage | AWS S3 + Glacier lifecycle |
| Cache | Upstash Redis |
| Dead-letter queue | AWS SQS |
| Observability | AWS CloudWatch |
| Frontend | React + Vite + Recharts |
| Deploy | Render (backend) + Vercel (frontend) |

## Local setup

> Setup instructions coming as the project builds out.

## Status

🚧 Under active development
