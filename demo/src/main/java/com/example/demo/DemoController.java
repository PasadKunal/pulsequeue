package com.example.demo;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

@RestController
public class DemoController {

    @GetMapping("/fast")
    public ResponseEntity<Map<String, String>> fast() throws InterruptedException {
        Thread.sleep(ThreadLocalRandom.current().nextLong(10, 50));
        return ResponseEntity.ok(Map.of("status", "ok", "endpoint", "fast"));
    }

    @GetMapping("/slow")
    public ResponseEntity<Map<String, String>> slow() throws InterruptedException {
        Thread.sleep(ThreadLocalRandom.current().nextLong(400, 700));
        return ResponseEntity.ok(Map.of("status", "ok", "endpoint", "slow"));
    }

    @GetMapping("/error")
    public ResponseEntity<Map<String, String>> error() throws InterruptedException {
        Thread.sleep(ThreadLocalRandom.current().nextLong(80, 150));
        return ResponseEntity.internalServerError().body(Map.of("status", "error", "endpoint", "error"));
    }

    // Simulates a latency spike - useful for triggering anomaly detection
    @GetMapping("/spike")
    public ResponseEntity<Map<String, String>> spike() throws InterruptedException {
        Thread.sleep(ThreadLocalRandom.current().nextLong(2000, 3000));
        return ResponseEntity.ok(Map.of("status", "ok", "endpoint", "spike"));
    }
}
