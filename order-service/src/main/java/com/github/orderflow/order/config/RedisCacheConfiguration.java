package com.github.orderflow.order.config;

import com.github.orderflow.order.api.OrderResponse;
import java.util.Map;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.cache.RedisCacheManager;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.serializer.JacksonJsonRedisSerializer;
import org.springframework.data.redis.serializer.RedisSerializationContext;
import tools.jackson.databind.ObjectMapper;

@Configuration(proxyBeanMethods = false)
public class RedisCacheConfiguration {

    public static final String ORDERS_CACHE = "orders";

    @Bean
    RedisCacheManager cacheManager(
            RedisConnectionFactory connectionFactory,
            ObjectMapper objectMapper,
            OrderCacheProperties properties) {
        var serializer = new JacksonJsonRedisSerializer<>(objectMapper, OrderResponse.class);
        var cacheConfiguration = org.springframework.data.redis.cache.RedisCacheConfiguration
                .defaultCacheConfig()
                .entryTtl(properties.ttl())
                .disableCachingNullValues()
                .serializeValuesWith(RedisSerializationContext.SerializationPair.fromSerializer(serializer));

        return RedisCacheManager.builder(connectionFactory)
                .cacheDefaults(cacheConfiguration)
                .withInitialCacheConfigurations(Map.of(ORDERS_CACHE, cacheConfiguration))
                .enableStatistics()
                .transactionAware()
                .build();
    }
}
