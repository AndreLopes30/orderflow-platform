package com.github.orderflow.order;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.github.orderflow.order.domain.OrderRepository;
import com.github.orderflow.order.domain.OrderStatus;
import com.github.orderflow.order.outbox.OutboxEventRepository;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import tools.jackson.databind.ObjectMapper;

@Testcontainers(disabledWithoutDocker = true)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class OrderFlowIntegrationIT {

    @Container
    static final PostgreSQLContainer POSTGRES =
            new PostgreSQLContainer(DockerImageName.parse("postgres:17-alpine"));

    @Container
    static final KafkaContainer KAFKA =
            new KafkaContainer(DockerImageName.parse("apache/kafka-native:4.1.2"));

    @Container
    static final GenericContainer<?> REDIS = new GenericContainer<>(DockerImageName.parse("redis:7.4-alpine"))
            .withExposedPorts(6379);

    @DynamicPropertySource
    static void infrastructure(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
        registry.add("spring.data.redis.host", REDIS::getHost);
        registry.add("spring.data.redis.port", () -> REDIS.getMappedPort(6379));
        registry.add("app.outbox.poll-interval", () -> "100ms");
        registry.add("management.tracing.export.enabled", () -> "false");
    }

    @LocalServerPort
    private int port;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private OutboxEventRepository outboxRepository;

    @Autowired
    private OrderRepository orderRepository;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Test
    void persistsPublishesCachesAndInvalidatesOrder() throws Exception {
        HttpClient client = HttpClient.newHttpClient();
        HttpResponse<String> created = client.send(
                HttpRequest.newBuilder(URI.create(baseUrl() + "/orders"))
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(
                                "{\"customerId\":\"customer-integration\",\"total\":150.50}"))
                        .build(),
                HttpResponse.BodyHandlers.ofString());

        assertThat(created.statusCode()).isEqualTo(201);
        UUID orderId = UUID.fromString(objectMapper.readTree(created.body()).get("id").stringValue());

        assertThat(jdbcTemplate.queryForObject(
                        "SELECT count(*) FROM flyway_schema_history WHERE success", Integer.class))
                .isPositive();
        assertThat(orderRepository.findById(orderId))
                .get()
                .satisfies(order -> {
                    assertThat(order.getCustomerId()).isEqualTo("customer-integration");
                    assertThat(order.getTotal()).isEqualByComparingTo("150.50");
                    assertThat(order.getStatus()).isEqualTo(OrderStatus.CREATED);
                });
        assertThat(outboxRepository.findAll())
                .singleElement()
                .satisfies(event -> {
                    assertThat(event.getAggregateId()).isEqualTo(orderId);
                    assertThat(event.getPayload()).contains(orderId.toString()).contains("\"eventVersion\":1");
                });

        try (var consumer = consumer()) {
            consumer.subscribe(java.util.List.of("order.created"));
            await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
                var records = consumer.poll(Duration.ofMillis(500));
                assertThat(records)
                        .anySatisfy(record -> {
                            assertThat(record.key()).isEqualTo(orderId.toString());
                            assertThat(record.value()).contains(orderId.toString()).contains("\"eventVersion\":1");
                        });
            });
        }

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                assertThat(outboxRepository.findAll())
                        .singleElement()
                        .extracting(event -> event.getPublishedAt())
                        .isNotNull());

        assertThat(get(client, "/orders/" + orderId).statusCode()).isEqualTo(200);
        assertThat(get(client, "/orders/" + orderId).statusCode()).isEqualTo(200);
        assertThat(REDIS.execInContainer("redis-cli", "EXISTS", "orders::" + orderId).getStdout().trim())
                .isEqualTo("1");

        HttpResponse<String> updated = client.send(
                HttpRequest.newBuilder(URI.create(baseUrl() + "/orders/" + orderId + "/status"))
                        .header("Content-Type", "application/json")
                        .method("PATCH", HttpRequest.BodyPublishers.ofString("{\"status\":\"PROCESSING\"}"))
                        .build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(updated.statusCode()).isEqualTo(200);
        assertThat(REDIS.execInContainer("redis-cli", "EXISTS", "orders::" + orderId).getStdout().trim())
                .isEqualTo("0");

        HttpResponse<String> refreshed = get(client, "/orders/" + orderId);
        assertThat(refreshed.statusCode()).isEqualTo(200);
        assertThat(objectMapper.readTree(refreshed.body()).get("status").stringValue()).isEqualTo("PROCESSING");
    }

    private HttpResponse<String> get(HttpClient client, String path) throws Exception {
        return client.send(
                HttpRequest.newBuilder(URI.create(baseUrl() + path)).GET().build(),
                HttpResponse.BodyHandlers.ofString());
    }

    private KafkaConsumer<String, String> consumer() {
        var properties = new java.util.Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "order-service-it-" + UUID.randomUUID());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return new KafkaConsumer<>(properties);
    }

    private String baseUrl() {
        return "http://localhost:" + port;
    }
}
