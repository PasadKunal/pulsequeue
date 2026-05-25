package com.pulsequeue.api;

import com.pulsequeue.aws.S3ArchivalService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
@RequestMapping("/v1/admin")
@RequiredArgsConstructor
public class ArchivalController {

    private final S3ArchivalService archivalService;

    @PostMapping("/archive")
    public ResponseEntity<Map<String, String>> triggerArchival() {
        archivalService.archiveHourlyMetrics();
        return ResponseEntity.ok(Map.of("status", "archival triggered"));
    }
}
