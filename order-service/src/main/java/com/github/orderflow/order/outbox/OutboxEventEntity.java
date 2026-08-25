package com.github.orderflow.order.outbox;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.Instant;
import java.util.UUID;
import org.hibernate.annotations.ColumnTransformer;

@Entity
@Table(name = "outbox_events")
public class OutboxEventEntity {

    @Id
    private UUID id;

    @Column(name = "aggregate_id", nullable = false)
    private UUID aggregateId;

    @Column(name = "aggregate_type", nullable = false, length = 50)
    private String aggregateType;

    @Column(name = "event_type", nullable = false, length = 100)
    private String eventType;

    @Column(nullable = false, columnDefinition = "jsonb")
    @ColumnTransformer(write = "?::jsonb")
    private String payload;

    @Column(name = "occurred_at", nullable = false)
    private Instant occurredAt;

    @Column(name = "published_at")
    private Instant publishedAt;

    @Column(nullable = false)
    private int attempts;

    @Column(name = "next_attempt_at", nullable = false)
    private Instant nextAttemptAt;

    @Column(name = "locked_at")
    private Instant lockedAt;

    @Column(name = "last_error", length = 1000)
    private String lastError;

    protected OutboxEventEntity() {
    }

    private OutboxEventEntity(OrderCreatedEvent event, String payload) {
        this.id = event.eventId();
        this.aggregateId = event.orderId();
        this.aggregateType = "Order";
        this.eventType = "OrderCreatedEvent";
        this.payload = payload;
        this.occurredAt = event.occurredAt();
        this.nextAttemptAt = event.occurredAt();
    }

    public static OutboxEventEntity pending(OrderCreatedEvent event, String payload) {
        return new OutboxEventEntity(event, payload);
    }

    public void claim(Instant now) {
        this.lockedAt = now;
    }

    public UUID getId() {
        return id;
    }

    public UUID getAggregateId() {
        return aggregateId;
    }

    public String getPayload() {
        return payload;
    }

    public int getAttempts() {
        return attempts;
    }

    public Instant getPublishedAt() {
        return publishedAt;
    }

    public String getLastError() {
        return lastError;
    }
}
