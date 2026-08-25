package com.github.orderflow.order.application;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.github.orderflow.order.api.CreateOrderRequest;
import com.github.orderflow.order.domain.OrderEntity;
import com.github.orderflow.order.domain.OrderRepository;
import com.github.orderflow.order.domain.OrderStatus;
import com.github.orderflow.order.outbox.OutboxEventEntity;
import com.github.orderflow.order.outbox.OutboxEventRepository;
import java.math.BigDecimal;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import tools.jackson.databind.ObjectMapper;

@ExtendWith(MockitoExtension.class)
class OrderCommandServiceTest {

    private static final Instant NOW = Instant.parse("2026-08-24T12:00:00Z");

    @Mock
    private OrderRepository orderRepository;

    @Mock
    private OutboxEventRepository outboxRepository;

    @Mock
    private ObjectMapper objectMapper;

    private OrderCommandService service;

    @BeforeEach
    void setUp() {
        service = new OrderCommandService(
                orderRepository,
                outboxRepository,
                objectMapper,
                Clock.fixed(NOW, ZoneOffset.UTC));
    }

    @Test
    void createsOrderAndOutboxEventTogether() {
        when(objectMapper.writeValueAsString(any())).thenReturn("{\"eventVersion\":1}");

        var response = service.create(new CreateOrderRequest(" customer-123 ", new BigDecimal("150.50")));

        assertThat(response.id()).isNotNull();
        assertThat(response.customerId()).isEqualTo("customer-123");
        assertThat(response.total()).isEqualByComparingTo("150.50");
        assertThat(response.status()).isEqualTo(OrderStatus.CREATED);
        assertThat(response.createdAt()).isEqualTo(NOW);

        ArgumentCaptor<OrderEntity> orderCaptor = ArgumentCaptor.forClass(OrderEntity.class);
        ArgumentCaptor<OutboxEventEntity> outboxCaptor = ArgumentCaptor.forClass(OutboxEventEntity.class);
        verify(orderRepository).save(orderCaptor.capture());
        verify(outboxRepository).save(outboxCaptor.capture());
        assertThat(outboxCaptor.getValue().getAggregateId()).isEqualTo(orderCaptor.getValue().getId());
        assertThat(outboxCaptor.getValue().getPayload()).contains("eventVersion");
    }

    @Test
    void rejectsInvalidStatusTransition() {
        UUID orderId = UUID.randomUUID();
        OrderEntity order = new OrderEntity(orderId, "customer-123", BigDecimal.TEN, NOW);
        when(orderRepository.findById(orderId)).thenReturn(Optional.of(order));

        assertThatThrownBy(() -> service.updateStatus(orderId, OrderStatus.COMPLETED))
                .isInstanceOf(InvalidOrderStatusTransitionException.class)
                .hasMessageContaining("CREATED")
                .hasMessageContaining("COMPLETED");
    }

    @Test
    void updatesStatusAndTimestamp() {
        UUID orderId = UUID.randomUUID();
        OrderEntity order = new OrderEntity(orderId, "customer-123", BigDecimal.TEN, NOW.minusSeconds(60));
        when(orderRepository.findById(orderId)).thenReturn(Optional.of(order));

        var response = service.updateStatus(orderId, OrderStatus.PROCESSING);

        assertThat(response.status()).isEqualTo(OrderStatus.PROCESSING);
        assertThat(response.updatedAt()).isEqualTo(NOW);
    }
}
