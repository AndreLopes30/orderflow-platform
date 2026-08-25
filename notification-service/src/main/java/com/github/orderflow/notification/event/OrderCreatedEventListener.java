package com.github.orderflow.notification.event;

import com.github.orderflow.notification.application.DeadLetterRecorder;
import com.github.orderflow.notification.application.NotificationProcessor;
import com.github.orderflow.notification.application.ProcessingResult;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.BackOff;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.retrytopic.DltStrategy;
import org.springframework.kafka.retrytopic.SameIntervalTopicReuseStrategy;
import org.springframework.kafka.retrytopic.TopicSuffixingStrategy;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

@Component
public class OrderCreatedEventListener {

    private static final Logger log = LoggerFactory.getLogger(OrderCreatedEventListener.class);

    private final ObjectMapper objectMapper;
    private final NotificationProcessor processor;
    private final DeadLetterRecorder deadLetterRecorder;
    private final Counter attemptsCounter;

    public OrderCreatedEventListener(
            ObjectMapper objectMapper,
            NotificationProcessor processor,
            DeadLetterRecorder deadLetterRecorder,
            MeterRegistry meterRegistry) {
        this.objectMapper = objectMapper;
        this.processor = processor;
        this.deadLetterRecorder = deadLetterRecorder;
        this.attemptsCounter = meterRegistry.counter("orderflow.notifications.attempts");
    }

    @RetryableTopic(
            attempts = "${app.kafka.retry.attempts}",
            backOff = @BackOff(
                    delayString = "${app.kafka.retry.initial-delay}",
                    multiplierString = "${app.kafka.retry.multiplier}",
                    maxDelayString = "${app.kafka.retry.max-delay}"),
            retryTopicSuffix = ".retry",
            dltTopicSuffix = ".dlt",
            topicSuffixingStrategy = TopicSuffixingStrategy.SUFFIX_WITH_INDEX_VALUE,
            sameIntervalTopicReuseStrategy = SameIntervalTopicReuseStrategy.MULTIPLE_TOPICS,
            dltStrategy = DltStrategy.FAIL_ON_ERROR,
            autoCreateTopics = "true",
            numPartitions = "${app.kafka.partitions}",
            replicationFactor = "${app.kafka.replicas}",
            exclude = InvalidOrderCreatedEventException.class)
    @KafkaListener(
            id = "order-created-notification-listener",
            topics = "${app.kafka.order-created-topic}",
            groupId = "${spring.kafka.consumer.group-id}")
    public void onOrderCreated(
            String payload,
            @Header(name = KafkaHeaders.RECEIVED_KEY, required = false) String kafkaKey) {
        attemptsCounter.increment();
        OrderCreatedEvent event = deserialize(payload);
        event.validate(kafkaKey);
        ProcessingResult result = processor.process(event);
        log.atInfo()
                .addKeyValue("eventId", event.eventId())
                .addKeyValue("orderId", event.orderId())
                .addKeyValue("result", result)
                .log("Handled OrderCreatedEvent");
    }

    @DltHandler
    public void onDeadLetter(String payload) {
        deadLetterRecorder.record(payload);
        log.atError()
                .addKeyValue("payloadLength", payload == null ? 0 : payload.length())
                .log("Recorded event from DLT");
    }

    private OrderCreatedEvent deserialize(String payload) {
        if (payload == null || payload.isBlank()) {
            throw new InvalidOrderCreatedEventException("OrderCreatedEvent payload is empty");
        }
        try {
            OrderCreatedEvent event = objectMapper.readValue(payload, OrderCreatedEvent.class);
            if (event == null) {
                throw new InvalidOrderCreatedEventException("OrderCreatedEvent payload is JSON null");
            }
            return event;
        } catch (RuntimeException exception) {
            if (exception instanceof InvalidOrderCreatedEventException invalidEvent) {
                throw invalidEvent;
            }
            throw new InvalidOrderCreatedEventException("OrderCreatedEvent payload is not valid JSON", exception);
        }
    }
}
