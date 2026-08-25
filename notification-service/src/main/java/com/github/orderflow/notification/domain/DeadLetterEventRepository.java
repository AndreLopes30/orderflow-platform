package com.github.orderflow.notification.domain;

import java.time.Instant;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

public interface DeadLetterEventRepository extends JpaRepository<DeadLetterEventEntity, UUID> {

    @Modifying
    @Query(value = """
            INSERT INTO dead_letter_events (
                id, deduplication_key, event_id, order_id, payload, failed_at, reason
            ) VALUES (
                :id, :deduplicationKey, :eventId, :orderId, :payload, :failedAt, :reason
            )
            ON CONFLICT (deduplication_key) DO NOTHING
            """, nativeQuery = true)
    int record(
            @Param("id") UUID id,
            @Param("deduplicationKey") String deduplicationKey,
            @Param("eventId") UUID eventId,
            @Param("orderId") UUID orderId,
            @Param("payload") String payload,
            @Param("failedAt") Instant failedAt,
            @Param("reason") String reason);

    long countByEventId(UUID eventId);
}
