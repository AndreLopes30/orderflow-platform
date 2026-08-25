package com.github.orderflow.notification.api;

import java.util.UUID;

public class NotificationNotFoundException extends RuntimeException {

    public NotificationNotFoundException(UUID orderId) {
        super("Notification for order " + orderId + " was not found");
    }
}
