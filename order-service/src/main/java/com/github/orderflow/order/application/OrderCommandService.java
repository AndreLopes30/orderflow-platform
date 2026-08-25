package com.github.orderflow.order.application;

import com.github.orderflow.order.api.CreateOrderRequest;
import com.github.orderflow.order.api.OrderResponse;
import com.github.orderflow.order.config.RedisCacheConfiguration;
import com.github.orderflow.order.domain.OrderEntity;
import com.github.orderflow.order.domain.OrderRepository;
import com.github.orderflow.order.domain.OrderStatus;
import com.github.orderflow.order.outbox.OrderCreatedEvent;
import com.github.orderflow.order.outbox.OutboxEventEntity;
import com.github.orderflow.order.outbox.OutboxEventRepository;
import java.time.Clock;
import java.time.Instant;
import java.util.UUID;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.ObjectMapper;

@Service
public class OrderCommandService {

    private final OrderRepository orderRepository;
    private final OutboxEventRepository outboxRepository;
    private final ObjectMapper objectMapper;
    private final Clock clock;

    public OrderCommandService(
            OrderRepository orderRepository,
            OutboxEventRepository outboxRepository,
            ObjectMapper objectMapper,
            Clock clock) {
        this.orderRepository = orderRepository;
        this.outboxRepository = outboxRepository;
        this.objectMapper = objectMapper;
        this.clock = clock;
    }

    @Transactional
    public OrderResponse create(CreateOrderRequest request) {
        Instant now = clock.instant();
        OrderEntity order = new OrderEntity(
                UUID.randomUUID(),
                request.customerId().trim(),
                request.total(),
                now);
        OrderCreatedEvent event = OrderCreatedEvent.from(order, UUID.randomUUID(), now);

        orderRepository.save(order);
        outboxRepository.save(OutboxEventEntity.pending(event, serialize(event)));
        return OrderResponse.from(order);
    }

    @CacheEvict(cacheNames = RedisCacheConfiguration.ORDERS_CACHE, key = "#orderId")
    @Transactional
    public OrderResponse updateStatus(UUID orderId, OrderStatus target) {
        OrderEntity order = orderRepository.findById(orderId)
                .orElseThrow(() -> new OrderNotFoundException(orderId));
        if (!order.getStatus().canTransitionTo(target)) {
            throw new InvalidOrderStatusTransitionException(orderId, order.getStatus(), target);
        }
        order.transitionTo(target, clock.instant());
        return OrderResponse.from(order);
    }

    private String serialize(OrderCreatedEvent event) {
        try {
            return objectMapper.writeValueAsString(event);
        } catch (RuntimeException exception) {
            throw new IllegalStateException("Could not serialize OrderCreatedEvent", exception);
        }
    }
}
