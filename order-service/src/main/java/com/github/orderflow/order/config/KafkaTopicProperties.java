package com.github.orderflow.order.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("app.kafka")
public record KafkaTopicProperties(String orderCreatedTopic, int partitions, int replicas) {

    public KafkaTopicProperties {
        if (orderCreatedTopic == null || orderCreatedTopic.isBlank()) {
            throw new IllegalArgumentException("app.kafka.order-created-topic must not be blank");
        }
        if (partitions < 1 || replicas < 1) {
            throw new IllegalArgumentException("Kafka partitions and replicas must be positive");
        }
    }
}
