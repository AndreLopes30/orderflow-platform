package com.github.orderflow.order.outbox;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.github.orderflow.order.config.KafkaTopicProperties;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;

@ExtendWith(MockitoExtension.class)
class OutboxPublisherTest {

    @Mock
    private OutboxStateService stateService;

    @Mock
    private KafkaTemplate<String, String> kafkaTemplate;

    private OutboxPublisher publisher;

    @BeforeEach
    void setUp() {
        publisher = new OutboxPublisher(
                stateService,
                kafkaTemplate,
                new KafkaTopicProperties("order.created", 3, 1),
                new OutboxProperties(
                        50,
                        Duration.ofSeconds(30),
                        Duration.ofSeconds(1),
                        Duration.ofSeconds(1),
                        Duration.ofMinutes(1)),
                new SimpleMeterRegistry());
    }

    @Test
    void marksAcknowledgedRecordAsPublished() {
        var event = new ClaimedOutboxEvent(UUID.randomUUID(), UUID.randomUUID(), "{}", 0);
        when(stateService.claimBatch()).thenReturn(List.of(event));
        when(kafkaTemplate.send("order.created", event.orderId().toString(), "{}"))
                .thenReturn(CompletableFuture.completedFuture(null));

        publisher.publishPendingEvents();

        verify(stateService).markPublished(event.eventId());
    }

    @Test
    void keepsFailedRecordForRetry() {
        var event = new ClaimedOutboxEvent(UUID.randomUUID(), UUID.randomUUID(), "{}", 0);
        var failed = new CompletableFuture<org.springframework.kafka.support.SendResult<String, String>>();
        failed.completeExceptionally(new IllegalStateException("broker unavailable"));
        when(stateService.claimBatch()).thenReturn(List.of(event));
        when(kafkaTemplate.send("order.created", event.orderId().toString(), "{}")).thenReturn(failed);

        publisher.publishPendingEvents();

        verify(stateService).markFailed(org.mockito.ArgumentMatchers.eq(event), org.mockito.ArgumentMatchers.any());
    }
}
