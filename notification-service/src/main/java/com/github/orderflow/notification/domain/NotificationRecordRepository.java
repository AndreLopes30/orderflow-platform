package com.github.orderflow.notification.domain;

import java.util.Optional;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;

public interface NotificationRecordRepository extends JpaRepository<NotificationRecordEntity, UUID> {

    Optional<NotificationRecordEntity> findByOrderId(UUID orderId);

    long countByEventId(UUID eventId);
}
