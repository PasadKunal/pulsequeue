package com.pulsequeue.api;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
@RequestMapping("/v1")
public class HealthController {

    @GetMapping("/ping")
    public Map<String, String> ping() {
        return Map.of(
            "status", "ok",
            "service", "pulsequeue",
            "version", "0.0.1"
        );
    }
}
