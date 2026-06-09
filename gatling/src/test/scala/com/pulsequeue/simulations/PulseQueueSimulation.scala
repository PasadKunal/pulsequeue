package com.pulsequeue.simulations

import io.gatling.core.Predef._
import io.gatling.http.Predef._

import scala.concurrent.duration._
import scala.util.Random

class PulseQueueSimulation extends Simulation {

  val baseUrl = sys.env.getOrElse("PULSEQUEUE_URL", "http://localhost:8080")

  val httpProtocol = http
    .baseUrl(baseUrl)
    .acceptHeader("application/json")
    .contentTypeHeader("application/json")
    .shareConnections

  // ── feeders ────────────────────────────────────────────────────────────────

  val services = Vector("payment-svc", "auth-svc", "order-svc", "inventory-svc", "notification-svc")

  /** Normal traffic: low latency, ~3% errors */
  val normalFeeder = Iterator.continually {
    val svc     = services(Random.nextInt(services.size))
    val latency = 20 + Random.nextInt(280)                       // 20–300 ms
    val status  = if (Random.nextDouble() < 0.03) 500 else 200
    Map("sourceId" -> svc, "latencyMs" -> latency, "statusCode" -> status)
  }

  /** Spike traffic: high latency, 50% errors — triggers z-score anomaly */
  val spikeFeeder = Iterator.continually {
    val svc     = services(Random.nextInt(services.size))
    val latency = 1500 + Random.nextInt(2000)                    // 1.5–3.5 s
    val status  = if (Random.nextDouble() < 0.5) 500 else 200
    Map("sourceId" -> svc, "latencyMs" -> latency, "statusCode" -> status)
  }

  // ── request bodies ─────────────────────────────────────────────────────────

  def singleEventBody =
    """[{"sourceId":"${sourceId}","latencyMs":${latencyMs},"statusCode":${statusCode}}]"""

  def batchEventBody = StringBody { _ =>
    val svc = services(Random.nextInt(services.size))
    val events = (1 to 5).map { _ =>
      val lat    = 20 + Random.nextInt(280)
      val status = if (Random.nextDouble() < 0.03) 500 else 200
      s"""{"sourceId":"$svc","latencyMs":$lat,"statusCode":$status}"""
    }.mkString("[", ",", "]")
    events
  }

  // ── scenarios ──────────────────────────────────────────────────────────────

  val normalIngestion = scenario("Normal Ingestion")
    .feed(normalFeeder)
    .exec(
      http("POST /v1/events - single")
        .post("/v1/events")
        .body(StringBody(singleEventBody))
        .check(status.is(200))
    )

  val batchIngestion = scenario("Batch Ingestion")
    .exec(
      http("POST /v1/events - batch")
        .post("/v1/events")
        .body(batchEventBody)
        .check(status.is(200))
    )

  val anomalySpike = scenario("Anomaly Spike")
    .feed(spikeFeeder)
    .exec(
      http("POST /v1/events - spike")
        .post("/v1/events")
        .body(StringBody(singleEventBody))
        .check(status.is(200))
    )

  val dashboardPolling = scenario("Dashboard Read")
    .exec(
      http("GET /v1/services")
        .get("/v1/services")
        .check(status.is(200))
    )
    .pause(2.seconds)
    .exec(
      http("GET /v1/metrics - payment-svc")
        .get("/v1/metrics?service=payment-svc")
        .check(status.is(200))
    )

  // ── injection plan ─────────────────────────────────────────────────────────
  //
  //  0:00–0:30  ramp 1→20 normal users          (warm-up)
  //  0:30–2:30  hold 20 normal + 5 batch        (steady state)
  //  2:30–3:00  ramp to 50 normal               (load increase)
  //  3:00–4:00  hold 50 normal + 10 batch       (peak load)
  //  4:00–5:00  spike: 30 anomaly users         (trigger z-score alerts)
  //  5:00–5:30  cool-down ramp to 5             (recovery)

  setUp(
    normalIngestion.inject(
      rampUsers(20).during(30.seconds),
      constantUsersPerSec(20).during(2.minutes),
      rampUsersPerSec(20).to(50).during(30.seconds),
      constantUsersPerSec(50).during(1.minute)
    ),

    batchIngestion.inject(
      nothingFor(30.seconds),
      constantUsersPerSec(5).during(2.minutes),
      nothingFor(30.seconds),
      constantUsersPerSec(10).during(1.minute)
    ),

    anomalySpike.inject(
      nothingFor(4.minutes),
      rampUsers(30).during(1.minute)
    ),

    dashboardPolling.inject(
      nothingFor(1.minute),
      constantUsersPerSec(2).during(4.minutes)
    )
  ).protocols(httpProtocol)
    .assertions(
      global.responseTime.percentile(95).lt(500),
      global.successfulRequests.percent.gt(95),
      forAll.responseTime.mean.lt(300)
    )
}
