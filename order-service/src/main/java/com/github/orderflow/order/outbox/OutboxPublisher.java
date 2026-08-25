package com.github.orderflow.order.outbox;

import com.github.orderflow.order.config.KafkaTopicProperties;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class OutboxPublisher {

    private static final Logger log = LoggerFactory.getLogger(OutboxPublisher.class);

    private final OutboxStateService stateService;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final KafkaTopicProperties kafkaProperties;
    private final OutboxProperties outboxProperties;
    private final Counter publishedCounter;
    private final Counter failureCounter;

    public OutboxPublisher(
            OutboxStateService stateService,
            KafkaTemplate<String, String> kafkaTemplate,
            KafkaTopicProperties kafkaProperties,
            OutboxProperties outboxProperties,
            MeterRegistry meterRegistry) {
        this.stateService = stateService;
        this.kafkaTemplate = kafkaTemplate;
        this.kafkaProperties = kafkaProperties;
        this.outboxProperties = outboxProperties;
        this.publishedCounter = meterRegistry.counter("orderflow.outbox.published");
        this.failureCounter = meterRegistry.counter("orderflow.outbox.failures");
    }

    @Scheduled(fixedDelayString = "${app.outbox.poll-interval:500ms}")
    public void publishPendingEvents() {
        for (ClaimedOutboxEvent event : stateService.claimBatch()) {
            publish(event);
        }
    }

    private void publish(ClaimedOutboxEvent event) {
        try {
            kafkaTemplate.send(
                            kafkaProperties.orderCreatedTopic(),
                            event.orderId().toString(),
                            event.payload())
                    .get(outboxProperties.sendTimeout().toMillis(), TimeUnit.MILLISECONDS);
            stateService.markPublished(event.eventId());
            publishedCounter.increment();
            log.atInfo()
                    .addKeyValue("eventId", event.eventId())
                    .addKeyValue("orderId", event.orderId())
                    .log("Published outbox event");
        } catch (Exception exception) {
            failureCounter.increment();
            stateService.markFailed(event, exception);
            log.atWarn()
                    .addKeyValue("eventId", event.eventId())
                    .addKeyValue("attempt", event.previousAttempts() + 1)
                    .setCause(exception)
                    .log("Could not publish outbox event; it remains pending");
        }
    }
}
