package com.github.orderflow.order.api;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.github.orderflow.order.application.OrderCommandService;
import com.github.orderflow.order.application.OrderNotFoundException;
import com.github.orderflow.order.application.OrderQueryService;
import com.github.orderflow.order.domain.OrderStatus;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

@ExtendWith(MockitoExtension.class)
class OrderControllerTest {

    @Mock
    private OrderCommandService commandService;

    @Mock
    private OrderQueryService queryService;

    private MockMvc mockMvc;

    @BeforeEach
    void setUp() {
        mockMvc = MockMvcBuilders
                .standaloneSetup(new OrderController(commandService, queryService))
                .setControllerAdvice(new ApiExceptionHandler())
                .build();
    }

    @Test
    void createsOrderWithLocation() throws Exception {
        UUID orderId = UUID.randomUUID();
        var response = new OrderResponse(
                orderId,
                "customer-123",
                new BigDecimal("150.50"),
                OrderStatus.CREATED,
                Instant.parse("2026-08-24T12:00:00Z"),
                Instant.parse("2026-08-24T12:00:00Z"));
        when(commandService.create(any())).thenReturn(response);

        mockMvc.perform(post("/orders")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"customerId":"customer-123","total":150.50}
                                """))
                .andExpect(status().isCreated())
                .andExpect(header().string("Location", "http://localhost/orders/" + orderId))
                .andExpect(jsonPath("$.id").value(orderId.toString()))
                .andExpect(jsonPath("$.status").value("CREATED"));
    }

    @Test
    void reportsValidationErrorsConsistently() throws Exception {
        mockMvc.perform(post("/orders")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"customerId":" ","total":0}
                                """))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.code").value("VALIDATION_ERROR"))
                .andExpect(jsonPath("$.errors.customerId").exists())
                .andExpect(jsonPath("$.errors.total").exists());
    }

    @Test
    void reportsMissingOrderAsProblemDetail() throws Exception {
        UUID orderId = UUID.randomUUID();
        when(queryService.findById(orderId)).thenThrow(new OrderNotFoundException(orderId));

        mockMvc.perform(get("/orders/{orderId}", orderId))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.code").value("ORDER_NOT_FOUND"));
    }

    @Test
    void reportsInvalidUuidAsProblemDetail() throws Exception {
        mockMvc.perform(get("/orders/not-a-uuid"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.code").value("INVALID_PATH_PARAMETER"));
    }
}
