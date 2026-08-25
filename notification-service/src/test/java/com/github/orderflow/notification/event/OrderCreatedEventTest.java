package com.github.orderflow.notification.event;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class OrderCreatedEventTest {

    @Test
    void acceptsSupportedEventWithOrderIdKey() {
        var event = validEvent();

        assertThatCode(() -> event.validate(event.orderId().toString())).doesNotThrowAnyException();
    }

    @Test
    void rejectsUnsupportedVersion() {
        var original = validEvent();
        var event = new OrderCreatedEvent(
                original.eventId(),
                2,
                original.occurredAt(),
                original.orderId(),
                original.customerId(),
                original.total());

        assertThatThrownBy(() -> event.validate(event.orderId().toString()))
                .isInstanceOf(InvalidOrderCreatedEventException.class)
                .hasMessageContaining("version");
    }

    @Test
    void rejectsMismatchedKafkaKey() {
        var event = validEvent();

        assertThatThrownBy(() -> event.validate(UUID.randomUUID().toString()))
                .isInstanceOf(InvalidOrderCreatedEventException.class)
                .hasMessageContaining("Kafka key");
    }

    static OrderCreatedEvent validEvent() {
        return new OrderCreatedEvent(
                UUID.randomUUID(),
                1,
                Instant.parse("2026-08-24T12:00:00Z"),
                UUID.randomUUID(),
                "customer-123",
                new BigDecimal("150.50"));
    }
}
