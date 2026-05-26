package com.pulsequeue.aws;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.pulsequeue.model.IngestEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.services.sqs.model.Message;

import java.util.List;

@Service
@RequiredArgsConstructor
@Slf4j
public class DlqRecoveryWorker {

    private final SqsDlqService sqsDlqService;
    private final KafkaTemplate<String, IngestEvent> kafkaTemplate;

    @Value("${pulsequeue.kafka.topics.events-raw:events.raw}")
    private String eventsRawTopic;

    private final ObjectMapper objectMapper = new ObjectMapper()
        .registerModule(new JavaTimeModule())
        .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

    @Scheduled(fixedDelay = 30000)
    public void recoverFailedEvents() {
        List<Message> messages = sqsDlqService.pollDlq(10);

        if (messages.isEmpty()) {
            log.debug("DLQ recovery: no messages to recover");
            return;
        }

        log.info("DLQ recovery: found {} messages to retry", messages.size());
        int recovered = 0;
        int failed = 0;

        for (Message message : messages) {
            try {
                IngestEvent event = objectMapper.readValue(message.body(), IngestEvent.class);

                kafkaTemplate.send(eventsRawTopic, event.getSourceId(), event)
                    .whenComplete((result, ex) -> {
                        if (ex == null) {
                            sqsDlqService.deleteFromDlq(message.receiptHandle());
                            log.debug("DLQ recovery: replayed event sourceId={}", event.getSourceId());
                        } else {
                            log.error("DLQ recovery: retry failed for sourceId={}: {}",
                                event.getSourceId(), ex.getMessage());
                        }
                    });
                recovered++;
            } catch (Exception e) {
                log.error("DLQ recovery: failed to deserialize message: {}", e.getMessage());
                failed++;
            }
        }

        log.info("DLQ recovery complete: recovered={} failed={}", recovered, failed);
    }
}
