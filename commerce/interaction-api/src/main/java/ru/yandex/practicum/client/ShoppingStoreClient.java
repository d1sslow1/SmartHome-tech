package ru.yandex.practicum.client;

import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.data.domain.Page;
import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.dto.ProductDto;

import java.util.List;
import java.util.UUID;

@FeignClient(name = "shopping-store", path = "/api/v1/shopping-store")
public interface ShoppingStoreClient {

    @GetMapping("/products")
    Page<ProductDto> getProducts(@RequestParam(defaultValue = "0") int page,
                                 @RequestParam(defaultValue = "10") int size);

    @GetMapping("/products/{productId}")
    ProductDto getProduct(@PathVariable("productId") UUID productId);

    @PostMapping("/products")
    ProductDto createProduct(@RequestBody ProductDto productDto);

    @PutMapping("/products/{productId}")
    ProductDto updateProduct(@PathVariable("productId") UUID productId,
                             @RequestBody ProductDto productDto);

    @DeleteMapping("/products/{productId}")
    void deleteProduct(@PathVariable("productId") UUID productId);

    @PostMapping("/products/{productId}/activate")
    void activateProduct(@PathVariable("productId") UUID productId);

    @PostMapping("/products/{productId}/deactivate")
    void deactivateProduct(@PathVariable("productId") UUID productId);

    @GetMapping("/products/category/{category}")
    List<ProductDto> getProductsByCategory(@PathVariable("category") String category);
}