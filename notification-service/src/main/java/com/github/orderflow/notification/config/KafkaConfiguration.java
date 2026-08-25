package com.github.orderflow.notification.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;

@Configuration(proxyBeanMethods = false)
public class KafkaConfiguration {

    @Bean
    NewTopic orderCreatedTopic(KafkaTopicProperties properties) {
        return TopicBuilder.name(properties.orderCreatedTopic())
                .partitions(properties.partitions())
                .replicas(properties.replicas())
                .config(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "1")
                .build();
    }

    @Bean
    SmartInitializingSingleton enableKafkaObservation(
            KafkaTemplate<String, String> kafkaTemplate,
            ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory) {
        return () -> {
            kafkaTemplate.setObservationEnabled(true);
            kafkaListenerContainerFactory.getContainerProperties().setObservationEnabled(true);
        };
    }
}
