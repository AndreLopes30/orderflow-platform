package com.github.orderflow.notification.event;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.github.orderflow.notification.application.DeadLetterRecorder;
import com.github.orderflow.notification.application.NotificationProcessor;
import com.github.orderflow.notification.application.ProcessingResult;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import tools.jackson.databind.ObjectMapper;

@ExtendWith(MockitoExtension.class)
class OrderCreatedEventListenerTest {

    @Mock
    private ObjectMapper objectMapper;

    @Mock
    private NotificationProcessor processor;

    @Mock
    private DeadLetterRecorder deadLetterRecorder;

    private OrderCreatedEventListener listener;

    @BeforeEach
    void setUp() {
        listener = new OrderCreatedEventListener(
                objectMapper,
                processor,
                deadLetterRecorder,
                new SimpleMeterRegistry());
    }

    @Test
    void deserializesValidatesAndProcessesEvent() {
        OrderCreatedEvent event = event();
        when(objectMapper.readValue("{}", OrderCreatedEvent.class)).thenReturn(event);
        when(processor.process(event)).thenReturn(ProcessingResult.PROCESSED);

        listener.onOrderCreated("{}", event.orderId().toString());

        verify(processor).process(event);
    }

    @Test
    void invalidJsonIsClassifiedAsNonRetryable() {
        when(objectMapper.readValue("invalid", OrderCreatedEvent.class))
                .thenThrow(new IllegalArgumentException("invalid JSON"));

        assertThatThrownBy(() -> listener.onOrderCreated("invalid", null))
                .isInstanceOf(InvalidOrderCreatedEventException.class);
    }

    @Test
    void delegatesDltPayloadToDurableRecorder() {
        listener.onDeadLetter("{}");

        verify(deadLetterRecorder).record("{}");
    }

    private OrderCreatedEvent event() {
        return new OrderCreatedEvent(
                UUID.randomUUID(),
                1,
                Instant.parse("2026-08-24T12:00:00Z"),
                UUID.randomUUID(),
                "customer-123",
                new BigDecimal("150.50"));
    }
}
