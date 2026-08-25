package com.github.orderflow.order.api;

import com.github.orderflow.order.domain.OrderStatus;
import jakarta.validation.constraints.NotNull;

public record UpdateOrderStatusRequest(@NotNull(message = "status is required") OrderStatus status) {
}
