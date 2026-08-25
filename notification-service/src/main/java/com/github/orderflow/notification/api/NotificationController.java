package com.github.orderflow.notification.api;

import com.github.orderflow.notification.domain.NotificationRecordRepository;
import java.util.UUID;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/notifications")
public class NotificationController {

    private final NotificationRecordRepository repository;

    public NotificationController(NotificationRecordRepository repository) {
        this.repository = repository;
    }

    @GetMapping("/orders/{orderId}")
    NotificationResponse findByOrderId(@PathVariable UUID orderId) {
        return repository.findByOrderId(orderId)
                .map(NotificationResponse::from)
                .orElseThrow(() -> new NotificationNotFoundException(orderId));
    }
}
