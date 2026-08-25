package com.github.orderflow.notification.domain;

import com.github.orderflow.notification.event.OrderCreatedEvent;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "notification_records")
public class NotificationRecordEntity {

    @Id
    private UUID id;

    @Column(name = "event_id", nullable = false, unique = true)
    private UUID eventId;

    @Column(name = "order_id", nullable = false)
    private UUID orderId;

    @Column(name = "customer_id", nullable = false, length = 100)
    private String customerId;

    @Column(nullable = false, length = 30)
    private String channel;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 20)
    private NotificationStatus status;

    @Column(name = "sent_at", nullable = false)
    private Instant sentAt;

    @Column(nullable = false, length = 255)
    private String message;

    protected NotificationRecordEntity() {
    }

    public static NotificationRecordEntity sent(OrderCreatedEvent event, Instant sentAt) {
        NotificationRecordEntity record = new NotificationRecordEntity();
        record.id = UUID.randomUUID();
        record.eventId = event.eventId();
        record.orderId = event.orderId();
        record.customerId = event.customerId();
        record.channel = "SIMULATED";
        record.status = NotificationStatus.SENT;
        record.sentAt = sentAt;
        record.message = "Order " + event.orderId() + " created successfully";
        return record;
    }

    public UUID getId() {
        return id;
    }

    public UUID getEventId() {
        return eventId;
    }

    public UUID getOrderId() {
        return orderId;
    }

    public String getCustomerId() {
        return customerId;
    }

    public String getChannel() {
        return channel;
    }

    public NotificationStatus getStatus() {
        return status;
    }

    public Instant getSentAt() {
        return sentAt;
    }

    public String getMessage() {
        return message;
    }
}
