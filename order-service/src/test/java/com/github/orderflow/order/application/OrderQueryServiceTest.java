package com.github.orderflow.order.application;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.when;

import com.github.orderflow.order.domain.OrderEntity;
import com.github.orderflow.order.domain.OrderRepository;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class OrderQueryServiceTest {

    @Mock
    private OrderRepository repository;

    @Test
    void returnsMappedOrder() {
        UUID orderId = UUID.randomUUID();
        OrderEntity order = new OrderEntity(orderId, "customer-123", new BigDecimal("10.25"), Instant.EPOCH);
        when(repository.findById(orderId)).thenReturn(Optional.of(order));

        var result = new OrderQueryService(repository).findById(orderId);

        assertThat(result.id()).isEqualTo(orderId);
        assertThat(result.total()).isEqualByComparingTo("10.25");
    }

    @Test
    void throwsTypedExceptionWhenMissing() {
        UUID orderId = UUID.randomUUID();
        when(repository.findById(orderId)).thenReturn(Optional.empty());

        assertThatThrownBy(() -> new OrderQueryService(repository).findById(orderId))
                .isInstanceOf(OrderNotFoundException.class)
                .hasMessageContaining(orderId.toString());
    }
}
