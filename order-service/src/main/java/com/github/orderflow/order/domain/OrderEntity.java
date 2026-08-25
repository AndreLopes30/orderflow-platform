package com.github.orderflow.order.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.Version;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "orders")
public class OrderEntity {

    @Id
    private UUID id;

    @Column(name = "customer_id", nullable = false, length = 100)
    private String customerId;

    @Column(nullable = false, precision = 19, scale = 2)
    private BigDecimal total;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 20)
    private OrderStatus status;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    @Version
    @Column(nullable = false)
    private long version;

    protected OrderEntity() {
    }

    public OrderEntity(UUID id, String customerId, BigDecimal total, Instant now) {
        this.id = id;
        this.customerId = customerId;
        this.total = total;
        this.status = OrderStatus.CREATED;
        this.createdAt = now;
        this.updatedAt = now;
    }

    public void transitionTo(OrderStatus target, Instant now) {
        if (!status.canTransitionTo(target)) {
            throw new IllegalStateException("Cannot transition order from " + status + " to " + target);
        }
        this.status = target;
        this.updatedAt = now;
    }

    public UUID getId() {
        return id;
    }

    public String getCustomerId() {
        return customerId;
    }

    public BigDecimal getTotal() {
        return total;
    }

    public OrderStatus getStatus() {
        return status;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public Instant getUpdatedAt() {
        return updatedAt;
    }
}
