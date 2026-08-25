package com.github.orderflow.order.outbox;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class OutboxStateService {

    private static final int MAX_ERROR_LENGTH = 1000;

    private final OutboxEventRepository repository;
    private final OutboxProperties properties;
    private final Clock clock;

    public OutboxStateService(OutboxEventRepository repository, OutboxProperties properties, Clock clock) {
        this.repository = repository;
        this.properties = properties;
        this.clock = clock;
    }

    @Transactional
    public List<ClaimedOutboxEvent> claimBatch() {
        Instant now = clock.instant();
        List<OutboxEventEntity> events = repository.lockNextBatch(
                now,
                now.minus(properties.lockTimeout()),
                properties.batchSize());
        events.forEach(event -> event.claim(now));
        return events.stream()
                .map(event -> new ClaimedOutboxEvent(
                        event.getId(),
                        event.getAggregateId(),
                        event.getPayload(),
                        event.getAttempts()))
                .toList();
    }

    @Transactional
    public void markPublished(UUID eventId) {
        repository.markPublished(eventId, clock.instant());
    }

    @Transactional
    public void markFailed(ClaimedOutboxEvent event, Throwable failure) {
        Instant now = clock.instant();
        Duration backoff = calculateBackoff(event.previousAttempts());
        String message = failure.getClass().getSimpleName() + ": " + String.valueOf(failure.getMessage());
        repository.markFailed(event.eventId(), now.plus(backoff), truncate(message));
    }

    private Duration calculateBackoff(int previousAttempts) {
        long multiplier = 1L << Math.min(previousAttempts, 20);
        Duration candidate;
        try {
            candidate = properties.initialBackoff().multipliedBy(multiplier);
        } catch (ArithmeticException exception) {
            candidate = properties.maxBackoff();
        }
        return candidate.compareTo(properties.maxBackoff()) > 0 ? properties.maxBackoff() : candidate;
    }

    private String truncate(String value) {
        return value.length() <= MAX_ERROR_LENGTH ? value : value.substring(0, MAX_ERROR_LENGTH);
    }
}
