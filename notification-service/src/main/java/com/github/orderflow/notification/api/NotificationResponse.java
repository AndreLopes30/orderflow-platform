package com.github.orderflow.notification.api;

import com.github.orderflow.notification.domain.NotificationRecordEntity;
import com.github.orderflow.notification.domain.NotificationStatus;
import java.time.Instant;
import java.util.UUID;

public record NotificationResponse(
        UUID id,
        UUID eventId,
        UUID orderId,
        String customerId,
        String channel,
        NotificationStatus status,
        Instant sentAt,
        String message) {

    static NotificationResponse from(NotificationRecordEntity entity) {
        return new NotificationResponse(
                entity.getId(),
                entity.getEventId(),
                entity.getOrderId(),
                entity.getCustomerId(),
                entity.getChannel(),
                entity.getStatus(),
                entity.getSentAt(),
                entity.getMessage());
    }
}
