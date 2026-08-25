package com.github.orderflow.notification.application;

import com.github.orderflow.notification.domain.DeadLetterEventRepository;
import com.github.orderflow.notification.event.OrderCreatedEvent;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Clock;
import java.util.HexFormat;
import java.util.UUID;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.ObjectMapper;

@Service
public class DeadLetterRecorder {

    private final DeadLetterEventRepository repository;
    private final ObjectMapper objectMapper;
    private final Clock clock;
    private final Counter dltCounter;

    public DeadLetterRecorder(
            DeadLetterEventRepository repository,
            ObjectMapper objectMapper,
            Clock clock,
            MeterRegistry meterRegistry) {
        this.repository = repository;
        this.objectMapper = objectMapper;
        this.clock = clock;
        this.dltCounter = meterRegistry.counter("orderflow.notifications.dlt");
    }

    @Transactional
    public void record(String payload) {
        String safePayload = payload == null ? "null" : payload;
        OrderCreatedEvent event = parseLeniently(safePayload);
        String deduplicationKey = event != null && event.eventId() != null
                ? event.eventId().toString()
                : sha256(safePayload);
        int inserted = repository.record(
                UUID.randomUUID(),
                deduplicationKey,
                event == null ? null : event.eventId(),
                event == null ? null : event.orderId(),
                safePayload,
                clock.instant(),
                "Retries exhausted or event classified as non-retryable");
        if (inserted == 1) {
            dltCounter.increment();
        }
    }

    private OrderCreatedEvent parseLeniently(String payload) {
        try {
            return objectMapper.readValue(payload, OrderCreatedEvent.class);
        } catch (RuntimeException exception) {
            return null;
        }
    }

    private String sha256(String payload) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256")
                    .digest(payload.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(digest);
        } catch (NoSuchAlgorithmException exception) {
            throw new IllegalStateException("SHA-256 is not available", exception);
        }
    }
}
