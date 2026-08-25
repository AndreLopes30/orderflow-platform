package com.github.orderflow.order.api;

import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.Digits;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;
import java.math.BigDecimal;

public record CreateOrderRequest(
        @NotBlank(message = "customerId is required")
        @Size(max = 100, message = "customerId must have at most 100 characters")
        String customerId,

        @DecimalMin(value = "0.0", inclusive = false, message = "total must be greater than zero")
        @Digits(integer = 17, fraction = 2, message = "total must have at most 17 integer and 2 decimal digits")
        BigDecimal total) {
}
