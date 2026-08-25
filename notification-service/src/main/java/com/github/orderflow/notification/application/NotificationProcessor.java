package com.github.orderflow.notification.application;

import com.github.orderflow.notification.domain.NotificationRecordEntity;
import com.github.orderflow.notification.domain.NotificationRecordRepository;
import com.github.orderflow.notification.domain.ProcessedEventRepository;
import com.github.orderflow.notification.event.OrderCreatedEvent;
import com.github.orderflow.notification.event.SimulatedNotificationException;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Clock;
import java.time.Instant;
import java.util.Locale;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class NotificationProcessor {

    static final String FAILURE_CUSTOMER_PREFIX = "fail-dlt-";

    private final ProcessedEventRepository processedEventRepository;
    private final NotificationRecordRepository notificationRepository;
    private final Clock clock;
    private final Counter processedCounter;
    private final Counter duplicateCounter;
    private final Counter simulatedFailureCounter;

    public NotificationProcessor(
            ProcessedEventRepository processedEventRepository,
            NotificationRecordRepository notificationRepository,
            Clock clock,
            MeterRegistry meterRegistry) {
        this.processedEventRepository = processedEventRepository;
        this.notificationRepository = notificationRepository;
        this.clock = clock;
        this.processedCounter = meterRegistry.counter("orderflow.notifications.processed");
        this.duplicateCounter = meterRegistry.counter("orderflow.notifications.duplicates");
        this.simulatedFailureCounter = meterRegistry.counter("orderflow.notifications.simulated_failures");
    }

    @Transactional
    public ProcessingResult process(OrderCreatedEvent event) {
        Instant now = clock.instant();
        if (processedEventRepository.claim(event.eventId(), now) == 0) {
            duplicateCounter.increment();
            return ProcessingResult.DUPLICATE;
        }

        simulateNotificationProvider(event);
        notificationRepository.save(NotificationRecordEntity.sent(event, now));
        processedCounter.increment();
        return ProcessingResult.PROCESSED;
    }

    private void simulateNotificationProvider(OrderCreatedEvent event) {
        if (event.customerId().toLowerCase(Locale.ROOT).startsWith(FAILURE_CUSTOMER_PREFIX)) {
            simulatedFailureCounter.increment();
            throw new SimulatedNotificationException(
                    "Controlled failure requested for customerId " + event.customerId());
        }
    }
}
