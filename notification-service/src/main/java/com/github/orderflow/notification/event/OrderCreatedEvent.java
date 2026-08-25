package com.github.orderflow.notification.event;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

public record OrderCreatedEvent(
        UUID eventId,
        int eventVersion,
        Instant occurredAt,
        UUID orderId,
        String customerId,
        BigDecimal total) {

    public static final int SUPPORTED_VERSION = 1;

    public void validate(String kafkaKey) {
        if (eventId == null || occurredAt == null || orderId == null) {
            throw new InvalidOrderCreatedEventException("Event identifiers and occurredAt are required");
        }
        if (eventVersion != SUPPORTED_VERSION) {
            throw new InvalidOrderCreatedEventException("Unsupported OrderCreatedEvent version: " + eventVersion);
        }
        if (customerId == null || customerId.isBlank()) {
            throw new InvalidOrderCreatedEventException("customerId is required");
        }
        if (total == null || total.signum() <= 0) {
            throw new InvalidOrderCreatedEventException("total must be positive");
        }
        if (kafkaKey == null || !orderId.toString().equals(kafkaKey)) {
            throw new InvalidOrderCreatedEventException("Kafka key must match orderId");
        }
    }
}
