package com.github.orderflow.notification.application;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.github.orderflow.notification.domain.DeadLetterEventRepository;
import com.github.orderflow.notification.event.OrderCreatedEvent;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.math.BigDecimal;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import tools.jackson.databind.ObjectMapper;

@ExtendWith(MockitoExtension.class)
class DeadLetterRecorderTest {

    private static final Instant NOW = Instant.parse("2026-08-24T12:00:00Z");

    @Mock
    private DeadLetterEventRepository repository;

    @Mock
    private ObjectMapper objectMapper;

    @Test
    void usesEventIdAsDeduplicationKey() {
        UUID eventId = UUID.randomUUID();
        UUID orderId = UUID.randomUUID();
        var event = new OrderCreatedEvent(eventId, 1, NOW, orderId, "customer-123", BigDecimal.TEN);
        when(objectMapper.readValue("{}", OrderCreatedEvent.class)).thenReturn(event);
        var recorder = new DeadLetterRecorder(
                repository,
                objectMapper,
                Clock.fixed(NOW, ZoneOffset.UTC),
                new SimpleMeterRegistry());

        recorder.record("{}");

        verify(repository).record(
                any(),
                eq(eventId.toString()),
                eq(eventId),
                eq(orderId),
                eq("{}"),
                eq(NOW),
                any());
    }
}
