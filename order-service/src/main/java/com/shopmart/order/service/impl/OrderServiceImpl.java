package com.shopmart.order.service.impl;

import com.shopmart.order.client.InventoryClient;
import com.shopmart.order.dto.OrderRequest;
import com.shopmart.order.dto.OrderResponse;
import com.shopmart.order.dto.ProductDto;
import com.shopmart.order.entity.Order;
import com.shopmart.order.entity.OrderStatus;
import com.shopmart.order.event.KafkaTopics;
import com.shopmart.order.event.OrderEvent;
import com.shopmart.order.event.SagaEventType;
import com.shopmart.order.exception.ResourceNotFoundException;
import com.shopmart.order.repository.OrderRepository;
import com.shopmart.order.service.OrderService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
public class OrderServiceImpl implements OrderService {

    private final OrderRepository orderRepository;
    private final InventoryClient inventoryClient;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    @Override
    @Transactional
    public OrderResponse createOrder(OrderRequest request) {
        // TODO Câu 2: Gọi inventory-service qua FeignClient (có Circuit Breaker fallback)
        log.info("Lấy thông tin sản phẩm từ inventory-service cho productId={}", request.getProductId());
        ProductDto product = inventoryClient.getProductById(request.getProductId());

        if (product == null || product.getPrice() == null || BigDecimal.ZERO.compareTo(product.getPrice()) >= 0) {
            log.error("Không thể lấy giá hoặc sản phẩm không khả dụng: productId={}", request.getProductId());
            throw new IllegalStateException("Dịch vụ kiểm tra kho đang bận hoặc sản phẩm không hợp lệ.");
        }

        // Tính tổng tiền = price * quantity
        BigDecimal totalAmount = product.getPrice().multiply(BigDecimal.valueOf(request.getQuantity()));

        // Lưu đơn hàng ở trạng thái PENDING
        Order order = Order.builder()
                .customerId(request.getCustomerId())
                .productId(request.getProductId())
                .quantity(request.getQuantity())
                .totalAmount(totalAmount)
                .status(OrderStatus.PENDING)
                .build();
        Order saved = orderRepository.save(order);
        log.info("Tạo thành công đơn hàng id={} trạng thái PENDING, totalAmount={}", saved.getId(), totalAmount);

        // TODO Câu 3: Khởi tạo và publish OrderEvent lên Kafka topic để kích hoạt Saga
        OrderEvent orderEvent = OrderEvent.builder()
                .orderId(saved.getId())
                .productId(saved.getProductId())
                .quantity(saved.getQuantity())
                .amount(saved.getTotalAmount())
                .type(SagaEventType.ORDER_CREATED)
                .message("Bắt đầu giao dịch Saga đặt hàng")
                .build();

        kafkaTemplate.send(KafkaTopics.ORDER, String.valueOf(saved.getId()), orderEvent);
        log.info("Đã gửi sự kiện ORDER_CREATED vào topic '{}' cho orderId={}", KafkaTopics.ORDER, saved.getId());

        return OrderResponse.from(saved);
    }

    @Override
    public OrderResponse getOrderById(Long id) {
        return OrderResponse.from(findOrder(id));
    }

    @Override
    public List<OrderResponse> getAllOrders() {
        return orderRepository.findAll().stream()
                .map(OrderResponse::from)
                .toList();
    }

    @Override
    @Transactional
    public OrderResponse completeOrder(Long orderId) {
        Order order = findOrder(orderId);
        order.setStatus(OrderStatus.COMPLETED);
        order.setFailureReason(null);
        log.info("Đơn hàng id={} đã COMPLETED", orderId);
        return OrderResponse.from(orderRepository.save(order));
    }

    @Override
    @Transactional
    public OrderResponse cancelOrder(Long orderId, String reason) {
        Order order = findOrder(orderId);
        order.setStatus(OrderStatus.CANCELLED);
        order.setFailureReason(reason);
        log.error("Đơn hàng id={} bị CANCELLED: {}", orderId, reason);
        return OrderResponse.from(orderRepository.save(order));
    }

    private Order findOrder(Long id) {
        return orderRepository.findById(id)
                .orElseThrow(() -> new ResourceNotFoundException("Không tìm thấy đơn hàng id=" + id));
    }
}