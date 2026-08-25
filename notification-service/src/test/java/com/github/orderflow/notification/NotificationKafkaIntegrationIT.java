package com.github.orderflow.notification;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.github.orderflow.notification.domain.DeadLetterEventRepository;
import com.github.orderflow.notification.domain.NotificationRecordRepository;
import com.github.orderflow.notification.domain.ProcessedEventRepository;
import com.github.orderflow.notification.event.OrderCreatedEvent;
import io.micrometer.core.instrument.MeterRegistry;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;
import tools.jackson.databind.ObjectMapper;

@Testcontainers(disabledWithoutDocker = true)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
class NotificationKafkaIntegrationIT {

    @Container
    static final PostgreSQLContainer POSTGRES =
            new PostgreSQLContainer(DockerImageName.parse("postgres:17-alpine"));

    @Container
    static final KafkaContainer KAFKA =
            new KafkaContainer(DockerImageName.parse("apache/kafka-native:4.1.2"));

    @DynamicPropertySource
    static void infrastructure(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
        registry.add("app.kafka.retry.initial-delay", () -> "100ms");
        registry.add("app.kafka.retry.max-delay", () -> "200ms");
        registry.add("management.tracing.export.enabled", () -> "false");
    }

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private ProcessedEventRepository processedEventRepository;

    @Autowired
    private NotificationRecordRepository notificationRepository;

    @Autowired
    private DeadLetterEventRepository deadLetterRepository;

    @Autowired
    private MeterRegistry meterRegistry;

    @Test
    void processesDuplicateEventOnlyOnce() throws Exception {
        OrderCreatedEvent event = event("customer-idempotency");
        String payload = objectMapper.writeValueAsString(event);
        double duplicatesBefore = meterRegistry.counter("orderflow.notifications.duplicates").count();

        kafkaTemplate.send("order.created", event.orderId().toString(), payload).get(10, TimeUnit.SECONDS);
        kafkaTemplate.send("order.created", event.orderId().toString(), payload).get(10, TimeUnit.SECONDS);

        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
            assertThat(processedEventRepository.existsById(event.eventId())).isTrue();
            assertThat(notificationRepository.countByEventId(event.eventId())).isEqualTo(1);
            assertThat(meterRegistry.counter("orderflow.notifications.duplicates").count() - duplicatesBefore)
                    .isEqualTo(1.0);
        });
    }

    @Test
    void retriesControlledFailureThenRecordsDltWithoutStoppingConsumer() throws Exception {
        OrderCreatedEvent failedEvent = event("fail-dlt-integration");
        double attemptsBefore = meterRegistry.counter("orderflow.notifications.attempts").count();
        double dltBefore = meterRegistry.counter("orderflow.notifications.dlt").count();

        try (var dltConsumer = consumer()) {
            dltConsumer.subscribe(java.util.List.of("order.created.dlt"));
            kafkaTemplate.send(
                            "order.created",
                            failedEvent.orderId().toString(),
                            objectMapper.writeValueAsString(failedEvent))
                    .get(10, TimeUnit.SECONDS);

            await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
                assertThat(deadLetterRepository.countByEventId(failedEvent.eventId())).isEqualTo(1);
                assertThat(processedEventRepository.existsById(failedEvent.eventId())).isFalse();
                assertThat(notificationRepository.countByEventId(failedEvent.eventId())).isZero();
            });
            await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                    assertThat(dltConsumer.poll(Duration.ofMillis(500)))
                            .anySatisfy(record -> {
                                assertThat(record.key()).isEqualTo(failedEvent.orderId().toString());
                                assertThat(record.value()).contains(failedEvent.eventId().toString());
                            }));
        }

        assertThat(meterRegistry.counter("orderflow.notifications.attempts").count() - attemptsBefore)
                .isEqualTo(3.0);
        assertThat(meterRegistry.counter("orderflow.notifications.dlt").count() - dltBefore)
                .isEqualTo(1.0);

        OrderCreatedEvent healthyEvent = event("customer-after-dlt");
        kafkaTemplate.send(
                        "order.created",
                        healthyEvent.orderId().toString(),
                        objectMapper.writeValueAsString(healthyEvent))
                .get(10, TimeUnit.SECONDS);
        await().atMost(Duration.ofSeconds(20)).untilAsserted(() ->
                assertThat(notificationRepository.countByEventId(healthyEvent.eventId())).isEqualTo(1));
    }

    private OrderCreatedEvent event(String customerId) {
        return new OrderCreatedEvent(
                UUID.randomUUID(),
                1,
                Instant.now(),
                UUID.randomUUID(),
                customerId,
                new BigDecimal("150.50"));
    }

    private KafkaConsumer<String, String> consumer() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "notification-dlt-it-" + UUID.randomUUID());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return new KafkaConsumer<>(properties);
    }
}
