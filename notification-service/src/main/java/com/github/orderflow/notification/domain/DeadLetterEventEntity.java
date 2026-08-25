package com.github.orderflow.notification.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "dead_letter_events")
public class DeadLetterEventEntity {

    @Id
    private UUID id;

    @Column(name = "deduplication_key", nullable = false, unique = true, length = 64)
    private String deduplicationKey;

    @Column(name = "event_id")
    private UUID eventId;

    @Column(name = "order_id")
    private UUID orderId;

    @Column(nullable = false, columnDefinition = "text")
    private String payload;

    @Column(name = "failed_at", nullable = false)
    private Instant failedAt;

    @Column(nullable = false, length = 500)
    private String reason;

    protected DeadLetterEventEntity() {
    }
}
