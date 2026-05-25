package com.pulsequeue.aws;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.pulsequeue.storage.MetricAggregateEntity;
import com.pulsequeue.storage.MetricAggregateRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.zip.GZIPOutputStream;

@Service
@Slf4j
public class S3ArchivalService {

    private final MetricAggregateRepository repository;
    private final S3Client s3Client;
    private final ObjectMapper objectMapper;

    @Value("${pulsequeue.aws.s3.bucket}")
    private String bucketName;

    @Value("${pulsequeue.aws.s3.archival-enabled:false}")
    private boolean archivalEnabled;

    public S3ArchivalService(
            MetricAggregateRepository repository,
            @Value("${pulsequeue.aws.access-key-id:#{null}}") String accessKeyId,
            @Value("${pulsequeue.aws.secret-access-key:#{null}}") String secretAccessKey,
            @Value("${pulsequeue.aws.region:us-east-1}") String region) {

        this.repository = repository;
        this.objectMapper = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

        if (accessKeyId != null && secretAccessKey != null) {
            this.s3Client = S3Client.builder()
                .region(Region.of(region))
                .credentialsProvider(StaticCredentialsProvider.create(
                    AwsBasicCredentials.create(accessKeyId, secretAccessKey)
                ))
                .build();
            log.info("S3 client initialized for bucket={} region={}", bucketName, region);
        } else {
            this.s3Client = null;
            log.warn("AWS credentials not configured - S3 archival disabled");
        }
    }

    @Scheduled(cron = "0 0 * * * *")
    public void archiveHourlyMetrics() {
        if (!archivalEnabled || s3Client == null) {
            log.debug("S3 archival skipped - not enabled or credentials missing");
            return;
        }

        Instant hourAgo = Instant.now().minus(1, ChronoUnit.HOURS);
        Instant twoHoursAgo = Instant.now().minus(2, ChronoUnit.HOURS);

        List<MetricAggregateEntity> records = repository
            .findAllSince(twoHoursAgo)
            .stream()
            .filter(r -> r.getWindowStart().isBefore(hourAgo))
            .toList();

        if (records.isEmpty()) {
            log.info("No records to archive for this hour");
            return;
        }

        String key = buildS3Key(hourAgo);

        try {
            byte[] compressed = compress(objectMapper.writeValueAsBytes(records));
            s3Client.putObject(
                PutObjectRequest.builder()
                    .bucket(bucketName)
                    .key(key)
                    .contentType("application/gzip")
                    .contentLength((long) compressed.length)
                    .build(),
                RequestBody.fromBytes(compressed)
            );
            log.info("Archived {} records to s3://{}/{}", records.size(), bucketName, key);
        } catch (Exception e) {
            log.error("S3 archival failed: {}", e.getMessage());
        }
    }

    private String buildS3Key(Instant instant) {
        DateTimeFormatter formatter = DateTimeFormatter
            .ofPattern("yyyy/MM/dd/HH")
            .withZone(ZoneOffset.UTC);
        return String.format("metrics/%s/aggregates.json.gz", formatter.format(instant));
    }

    private byte[] compress(byte[] data) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try (GZIPOutputStream gzip = new GZIPOutputStream(bos)) {
            gzip.write(data);
        }
        return bos.toByteArray();
    }
}
