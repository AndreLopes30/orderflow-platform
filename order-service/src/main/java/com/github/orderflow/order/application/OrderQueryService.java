package com.github.orderflow.order.application;

import com.github.orderflow.order.api.OrderResponse;
import com.github.orderflow.order.config.RedisCacheConfiguration;
import com.github.orderflow.order.domain.OrderRepository;
import java.util.UUID;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class OrderQueryService {

    private final OrderRepository orderRepository;

    public OrderQueryService(OrderRepository orderRepository) {
        this.orderRepository = orderRepository;
    }

    @Cacheable(cacheNames = RedisCacheConfiguration.ORDERS_CACHE, key = "#orderId")
    @Transactional(readOnly = true)
    public OrderResponse findById(UUID orderId) {
        return orderRepository.findById(orderId)
                .map(OrderResponse::from)
                .orElseThrow(() -> new OrderNotFoundException(orderId));
    }
}
