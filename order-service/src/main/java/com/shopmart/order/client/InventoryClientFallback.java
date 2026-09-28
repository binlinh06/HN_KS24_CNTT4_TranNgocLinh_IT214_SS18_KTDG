package com.shopmart.order.client;

import com.shopmart.order.dto.ProductDto;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;

@Slf4j
@Component
public class InventoryClientFallback implements InventoryClient {

    @Override
    public ProductDto getProductById(Long productId) {
        log.warn("Fallback triggered: Inventory service is unavailable for productId={}", productId);
        return ProductDto.builder()
                .id(productId)
                .name("Unknown (Service Down)")
                .price(BigDecimal.ZERO)
                .stock(0)
                .build();
    }
}