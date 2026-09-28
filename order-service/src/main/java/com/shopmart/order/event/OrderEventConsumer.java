package com.shopmart.order.event;

import com.shopmart.order.service.OrderService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class OrderEventConsumer {

    private final OrderService orderService;

    @KafkaListener(topics = KafkaTopics.ORDER, groupId = "order-group")
    public void handleOrderEvent(OrderEvent event) {
        if (event == null || event.getType() == null) {
            return;
        }

        log.info("Received event {} for orderId={}", event.getType(), event.getOrderId());

        switch (event.getType()) {
            // Khi thanh toán xong (hoặc bước cuối cùng thành công) -> Đổi sang COMPLETED
            case PAYMENT_COMPLETED: // hoặc PAYMENT_COMPLETED tùy theo hằng số trong SagaEventType
                log.info("Saga successful! Completing order id={}", event.getOrderId());
                orderService.completeOrder(event.getOrderId());
                break;

            // Khi trừ kho thất bại hoặc thanh toán lỗi -> CANCELLED đơn hàng (Rollback)
            case INVENTORY_FAILED:
            case PAYMENT_FAILED:
                String reason = event.getMessage() != null ? event.getMessage() : "Saga transaction failed at " + event.getType();
                log.warn("Saga failed! Cancelling order id={}, reason: {}", event.getOrderId(), reason);
                orderService.cancelOrder(event.getOrderId(), reason);
                break;

            default:
                // Bỏ qua các sự kiện nội bộ khác như ORDER_CREATED
                break;
        }
    }
}