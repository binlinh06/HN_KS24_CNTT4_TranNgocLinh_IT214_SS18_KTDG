package com.shopmart.order.client;

import com.shopmart.order.dto.ProductDto;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;

@FeignClient(name = "inventory-service", fallback = InventoryClientFallback.class)
public interface InventoryClient {

    @GetMapping("/api/inventory/products/{productId}")
    ProductDto getProductById(@PathVariable("productId") Long productId);
}