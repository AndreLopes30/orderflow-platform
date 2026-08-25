package com.github.orderflow.notification.domain;

import java.time.Instant;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

public interface ProcessedEventRepository extends JpaRepository<ProcessedEventEntity, UUID> {

    @Modifying
    @Query(value = """
            INSERT INTO processed_events (event_id, processed_at)
            VALUES (:eventId, :processedAt)
            ON CONFLICT (event_id) DO NOTHING
            """, nativeQuery = true)
    int claim(@Param("eventId") UUID eventId, @Param("processedAt") Instant processedAt);
}
