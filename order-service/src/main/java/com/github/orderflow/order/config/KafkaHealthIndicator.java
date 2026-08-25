package com.github.orderflow.order.config;

import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthIndicator;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class KafkaHealthIndicator implements HealthIndicator {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final KafkaTopicProperties properties;

    public KafkaHealthIndicator(
            KafkaTemplate<String, String> kafkaTemplate,
            KafkaTopicProperties properties) {
        this.kafkaTemplate = kafkaTemplate;
        this.properties = properties;
    }

    @Override
    public Health health() {
        try {
            var partitions = kafkaTemplate.partitionsFor(properties.orderCreatedTopic());
            if (partitions == null || partitions.isEmpty()) {
                return Health.down().withDetail("reason", "Topic has no partitions").build();
            }
            return Health.up()
                    .withDetail("topic", properties.orderCreatedTopic())
                    .withDetail("partitions", partitions.size())
                    .build();
        } catch (RuntimeException exception) {
            return Health.down(exception).build();
        }
    }
}
