package com.github.orderflow.order.outbox;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class OutboxStateServiceTest {

    private static final Instant NOW = Instant.parse("2026-08-24T12:00:00Z");

    @Mock
    private OutboxEventRepository repository;

    @Test
    void claimsAvailableBatch() {
        var properties = properties();
        var event = eventEntity(0);
        when(repository.lockNextBatch(NOW, NOW.minusSeconds(30), 50)).thenReturn(List.of(event));
        var service = new OutboxStateService(repository, properties, fixedClock());

        var claimed = service.claimBatch();

        assertThat(claimed).hasSize(1);
        assertThat(claimed.getFirst().eventId()).isEqualTo(event.getId());
    }

    @Test
    void usesExponentialBackoffCappedAtMaximum() {
        var service = new OutboxStateService(repository, properties(), fixedClock());
        var event = new ClaimedOutboxEvent(UUID.randomUUID(), UUID.randomUUID(), "{}", 10);

        service.markFailed(event, new IllegalStateException("broker unavailable"));

        ArgumentCaptor<Instant> nextAttempt = ArgumentCaptor.forClass(Instant.class);
        verify(repository).markFailed(
                org.mockito.ArgumentMatchers.eq(event.eventId()),
                nextAttempt.capture(),
                org.mockito.ArgumentMatchers.contains("broker unavailable"));
        assertThat(nextAttempt.getValue()).isEqualTo(NOW.plusSeconds(60));
    }

    private OutboxProperties properties() {
        return new OutboxProperties(
                50,
                Duration.ofSeconds(30),
                Duration.ofSeconds(5),
                Duration.ofSeconds(1),
                Duration.ofMinutes(1));
    }

    private Clock fixedClock() {
        return Clock.fixed(NOW, ZoneOffset.UTC);
    }

    private OutboxEventEntity eventEntity(int attempts) {
        UUID orderId = UUID.randomUUID();
        var event = new OrderCreatedEvent(
                UUID.randomUUID(),
                1,
                NOW,
                orderId,
                "customer-123",
                BigDecimal.TEN);
        return OutboxEventEntity.pending(event, "{}");
    }
}
