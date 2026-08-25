package com.github.orderflow.order.outbox;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("app.outbox")
public record OutboxProperties(
        int batchSize,
        Duration lockTimeout,
        Duration sendTimeout,
        Duration initialBackoff,
        Duration maxBackoff) {

    public OutboxProperties {
        if (batchSize < 1) {
            throw new IllegalArgumentException("Outbox batch size must be positive");
        }
        if (lockTimeout == null || sendTimeout == null || initialBackoff == null || maxBackoff == null) {
            throw new IllegalArgumentException("Outbox durations must be configured");
        }
    }
}
