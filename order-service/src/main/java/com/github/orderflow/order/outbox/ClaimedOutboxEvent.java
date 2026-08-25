package com.github.orderflow.order.outbox;

import java.util.UUID;

record ClaimedOutboxEvent(UUID eventId, UUID orderId, String payload, int previousAttempts) {
}
