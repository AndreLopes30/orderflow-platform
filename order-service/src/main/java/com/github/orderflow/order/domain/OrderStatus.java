package com.github.orderflow.order.domain;

import java.util.EnumSet;
import java.util.Set;

public enum OrderStatus {
    CREATED,
    PROCESSING,
    COMPLETED,
    CANCELLED;

    public boolean canTransitionTo(OrderStatus target) {
        Set<OrderStatus> allowed = switch (this) {
            case CREATED -> EnumSet.of(PROCESSING, CANCELLED);
            case PROCESSING -> EnumSet.of(COMPLETED, CANCELLED);
            case COMPLETED, CANCELLED -> EnumSet.noneOf(OrderStatus.class);
        };
        return allowed.contains(target);
    }
}
