package com.github.orderflow.order.api;

import com.github.orderflow.order.domain.OrderEntity;
import com.github.orderflow.order.domain.OrderStatus;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

public record OrderResponse(
        UUID id,
        String customerId,
        BigDecimal total,
        OrderStatus status,
        Instant createdAt,
        Instant updatedAt) {

    public static OrderResponse from(OrderEntity order) {
        return new OrderResponse(
                order.getId(),
                order.getCustomerId(),
                order.getTotal(),
                order.getStatus(),
                order.getCreatedAt(),
                order.getUpdatedAt());
    }
}
