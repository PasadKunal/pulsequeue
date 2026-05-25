package com.pulsequeue.api;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@SpringBootTest
@AutoConfigureMockMvc
class EventControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @Test
    void ingestEvents_validBatch_returns200() throws Exception {
        String payload = """
            [
              {
                "sourceId": "payments-service",
                "eventType": "http_request",
                "latencyMs": 142,
                "statusCode": 200
              },
              {
                "sourceId": "payments-service",
                "eventType": "http_request",
                "latencyMs": 89,
                "statusCode": 201
              }
            ]
            """;

        mockMvc.perform(post("/v1/events")
                .contentType(MediaType.APPLICATION_JSON)
                .content(payload))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.received").value(2))
            .andExpect(jsonPath("$.status").value("accepted"));
    }

    @Test
    void ingestEvents_emptyBatch_returns400() throws Exception {
        mockMvc.perform(post("/v1/events")
                .contentType(MediaType.APPLICATION_JSON)
                .content("[]"))
            .andExpect(status().isBadRequest())
            .andExpect(jsonPath("$.error").exists());
    }
}
