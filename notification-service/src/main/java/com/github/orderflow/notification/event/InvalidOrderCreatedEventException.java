package com.github.orderflow.notification.event;

public class InvalidOrderCreatedEventException extends RuntimeException {

    public InvalidOrderCreatedEventException(String message) {
        super(message);
    }

    public InvalidOrderCreatedEventException(String message, Throwable cause) {
        super(message, cause);
    }
}
