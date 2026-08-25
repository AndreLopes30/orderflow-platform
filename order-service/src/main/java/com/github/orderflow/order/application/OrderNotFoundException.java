package com.github.orderflow.order.application;

import java.util.UUID;

public class OrderNotFoundException extends RuntimeException {

    public OrderNotFoundException(UUID orderId) {
        super("Order " + orderId + " was not found");
    }
}
