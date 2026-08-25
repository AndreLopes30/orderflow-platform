package com.github.orderflow.order.outbox;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

public interface OutboxEventRepository extends JpaRepository<OutboxEventEntity, UUID> {

    @Query(value = """
            SELECT *
              FROM outbox_events
             WHERE published_at IS NULL
               AND next_attempt_at <= :now
               AND (locked_at IS NULL OR locked_at < :lockExpiredBefore)
             ORDER BY occurred_at
             LIMIT :batchSize
             FOR UPDATE SKIP LOCKED
            """, nativeQuery = true)
    List<OutboxEventEntity> lockNextBatch(
            @Param("now") Instant now,
            @Param("lockExpiredBefore") Instant lockExpiredBefore,
            @Param("batchSize") int batchSize);

    @Modifying
    @Query("""
            UPDATE OutboxEventEntity event
               SET event.publishedAt = :publishedAt,
                   event.lockedAt = null,
                   event.lastError = null
             WHERE event.id = :eventId
               AND event.publishedAt IS NULL
            """)
    int markPublished(@Param("eventId") UUID eventId, @Param("publishedAt") Instant publishedAt);

    @Modifying
    @Query("""
            UPDATE OutboxEventEntity event
               SET event.attempts = event.attempts + 1,
                   event.nextAttemptAt = :nextAttemptAt,
                   event.lockedAt = null,
                   event.lastError = :error
             WHERE event.id = :eventId
               AND event.publishedAt IS NULL
            """)
    int markFailed(
            @Param("eventId") UUID eventId,
            @Param("nextAttemptAt") Instant nextAttemptAt,
            @Param("error") String error);

    long countByPublishedAtIsNull();
}
