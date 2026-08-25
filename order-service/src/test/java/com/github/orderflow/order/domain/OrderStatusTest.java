package com.github.orderflow.order.domain;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class OrderStatusTest {

    @Test
    void allowsOnlyForwardOrCancellationTransitions() {
        assertThat(OrderStatus.CREATED.canTransitionTo(OrderStatus.PROCESSING)).isTrue();
        assertThat(OrderStatus.CREATED.canTransitionTo(OrderStatus.CANCELLED)).isTrue();
        assertThat(OrderStatus.PROCESSING.canTransitionTo(OrderStatus.COMPLETED)).isTrue();
        assertThat(OrderStatus.PROCESSING.canTransitionTo(OrderStatus.CANCELLED)).isTrue();

        assertThat(OrderStatus.CREATED.canTransitionTo(OrderStatus.COMPLETED)).isFalse();
        assertThat(OrderStatus.COMPLETED.canTransitionTo(OrderStatus.CREATED)).isFalse();
        assertThat(OrderStatus.CANCELLED.canTransitionTo(OrderStatus.PROCESSING)).isFalse();
    }
}
