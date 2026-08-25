package com.github.orderflow.notification.application;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.github.orderflow.notification.domain.NotificationRecordRepository;
import com.github.orderflow.notification.domain.ProcessedEventRepository;
import com.github.orderflow.notification.event.OrderCreatedEvent;
import com.github.orderflow.notification.event.SimulatedNotificationException;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.math.BigDecimal;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class NotificationProcessorTest {

    private static final Instant NOW = Instant.parse("2026-08-24T12:00:00Z");

    @Mock
    private ProcessedEventRepository processedEventRepository;

    @Mock
    private NotificationRecordRepository notificationRepository;

    private NotificationProcessor processor;

    @BeforeEach
    void setUp() {
        processor = new NotificationProcessor(
                processedEventRepository,
                notificationRepository,
                Clock.fixed(NOW, ZoneOffset.UTC),
                new SimpleMeterRegistry());
    }

    @Test
    void recordsNotificationAfterClaimingEvent() {
        OrderCreatedEvent event = event("customer-123");
        when(processedEventRepository.claim(event.eventId(), NOW)).thenReturn(1);

        ProcessingResult result = processor.process(event);

        assertThat(result).isEqualTo(ProcessingResult.PROCESSED);
        verify(notificationRepository).save(any());
    }

    @Test
    void skipsDuplicateEventWithoutRepeatingEffect() {
        OrderCreatedEvent event = event("customer-123");
        when(processedEventRepository.claim(event.eventId(), NOW)).thenReturn(0);

        ProcessingResult result = processor.process(event);

        assertThat(result).isEqualTo(ProcessingResult.DUPLICATE);
        verify(notificationRepository, never()).save(any());
    }

    @Test
    void controlledFailureHappensAfterClaimAndBeforeEffect() {
        OrderCreatedEvent event = event("fail-dlt-demo");
        when(processedEventRepository.claim(event.eventId(), NOW)).thenReturn(1);

        assertThatThrownBy(() -> processor.process(event))
                .isInstanceOf(SimulatedNotificationException.class)
                .hasMessageContaining("fail-dlt-demo");
        verify(notificationRepository, never()).save(any());
    }

    private OrderCreatedEvent event(String customerId) {
        return new OrderCreatedEvent(
                UUID.randomUUID(),
                1,
                NOW,
                UUID.randomUUID(),
                customerId,
                new BigDecimal("150.50"));
    }
}
