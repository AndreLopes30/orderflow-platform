package com.github.orderflow.order.config;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("app.cache.orders")
public record OrderCacheProperties(Duration ttl) {

    public OrderCacheProperties {
        if (ttl == null || ttl.isNegative() || ttl.isZero()) {
            throw new IllegalArgumentException("Order cache TTL must be positive");
        }
    }
}
