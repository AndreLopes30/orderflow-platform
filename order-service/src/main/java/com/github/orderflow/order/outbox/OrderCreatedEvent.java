package com.github.orderflow.order.outbox;

import com.github.orderflow.order.domain.OrderEntity;
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

    public static final int CURRENT_VERSION = 1;

    public static OrderCreatedEvent from(OrderEntity order, UUID eventId, Instant occurredAt) {
        return new OrderCreatedEvent(
                eventId,
                CURRENT_VERSION,
                occurredAt,
                order.getId(),
                order.getCustomerId(),
                order.getTotal());
    }
}
