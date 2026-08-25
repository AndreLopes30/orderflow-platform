package com.github.orderflow.order.application;

import com.github.orderflow.order.domain.OrderStatus;
import java.util.UUID;

public class InvalidOrderStatusTransitionException extends RuntimeException {

    public InvalidOrderStatusTransitionException(UUID orderId, OrderStatus current, OrderStatus target) {
        super("Order " + orderId + " cannot transition from " + current + " to " + target);
    }
}
