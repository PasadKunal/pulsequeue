package com.pulsequeue.aws;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.pulsequeue.model.IngestEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.util.List;

@Service
@Slf4j
public class SqsDlqService {

    private final SqsClient sqsClient;
    private final ObjectMapper objectMapper;

    @Value("${pulsequeue.aws.sqs.dlq-url:#{null}}")
    private String dlqUrl;

    @Value("${pulsequeue.aws.sqs.dlq-enabled:false}")
    private boolean dlqEnabled;

    public SqsDlqService(
            @Value("${pulsequeue.aws.access-key-id:#{null}}") String accessKeyId,
            @Value("${pulsequeue.aws.secret-access-key:#{null}}") String secretAccessKey,
            @Value("${pulsequeue.aws.region:us-east-1}") String region) {

        this.objectMapper = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

        if (accessKeyId != null && secretAccessKey != null) {
            this.sqsClient = SqsClient.builder()
                .region(Region.of(region))
                .credentialsProvider(StaticCredentialsProvider.create(
                    AwsBasicCredentials.create(accessKeyId, secretAccessKey)
                ))
                .build();
            log.info("SQS client initialized for region={}", region);
        } else {
            this.sqsClient = null;
            log.warn("AWS credentials not configured - SQS DLQ disabled");
        }
    }

    public void sendToDlq(IngestEvent event, String errorReason) {
        if (!dlqEnabled || sqsClient == null || dlqUrl == null) {
            log.debug("SQS DLQ skipped - not enabled or not configured");
            return;
        }
        try {
            String body = objectMapper.writeValueAsString(event);
            sqsClient.sendMessage(SendMessageRequest.builder()
                .queueUrl(dlqUrl)
                .messageBody(body)
                .messageAttributes(java.util.Map.of(
                    "errorReason", MessageAttributeValue.builder()
                        .dataType("String")
                        .stringValue(errorReason)
                        .build(),
                    "sourceId", MessageAttributeValue.builder()
                        .dataType("String")
                        .stringValue(event.getSourceId())
                        .build()
                ))
                .build());
            log.warn("Sent failed event to DLQ: sourceId={} reason={}", event.getSourceId(), errorReason);
        } catch (Exception e) {
            log.error("Failed to send event to DLQ: {}", e.getMessage());
        }
    }

    public List<Message> pollDlq(int maxMessages) {
        if (!dlqEnabled || sqsClient == null || dlqUrl == null) {
            return List.of();
        }
        try {
            ReceiveMessageResponse response = sqsClient.receiveMessage(
                ReceiveMessageRequest.builder()
                    .queueUrl(dlqUrl)
                    .maxNumberOfMessages(maxMessages)
                    .waitTimeSeconds(5)
                    .messageAttributeNames("All")
                    .build()
            );
            return response.messages();
        } catch (Exception e) {
            log.error("Failed to poll DLQ: {}", e.getMessage());
            return List.of();
        }
    }

    public void deleteFromDlq(String receiptHandle) {
        if (sqsClient == null || dlqUrl == null) return;
        try {
            sqsClient.deleteMessage(DeleteMessageRequest.builder()
                .queueUrl(dlqUrl)
                .receiptHandle(receiptHandle)
                .build());
        } catch (Exception e) {
            log.error("Failed to delete message from DLQ: {}", e.getMessage());
        }
    }
}
