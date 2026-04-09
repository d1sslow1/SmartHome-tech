package ru.yandex.practicum.api;

import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.dto.ProductDto;
import ru.yandex.practicum.enums.ProductAvailability;
import ru.yandex.practicum.enums.ProductCategory;

import java.util.List;

public interface StoreApi {
    @GetMapping("/api/v1/shopping-store/products/{id}")
    ProductDto getProduct(@PathVariable Long id);

    @GetMapping("/api/v1/shopping-store/products")
    List<ProductDto> getProducts(@RequestParam(required = false) ProductCategory category);

    @PostMapping("/api/v1/shopping-store/products")
    ProductDto addProduct(@RequestBody ProductDto product);

    @PutMapping("/api/v1/shopping-store/products")
    ProductDto updateProduct(@RequestBody ProductDto product);

    @DeleteMapping("/api/v1/shopping-store/products/{id}")
    void deactivateProduct(@PathVariable Long id);

    @PutMapping("/api/v1/shopping-store/products/quantityState")
    void updateQuantityState(@RequestParam Long productId, @RequestParam ProductAvailability quantityState);
}