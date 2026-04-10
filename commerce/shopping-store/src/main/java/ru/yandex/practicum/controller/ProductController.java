package ru.yandex.practicum.controller;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.PageRequest;
import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.dto.ProductDto;
import ru.yandex.practicum.enums.ProductAvailability;
import ru.yandex.practicum.enums.ProductCategory;
import ru.yandex.practicum.service.ProductService;

import java.util.List;

@RestController
public class ProductController {

    private final ProductService productService;

    public ProductController(ProductService productService) {
        this.productService = productService;
    }

    @GetMapping("/api/v1/shopping-store/{id}")
    public ProductDto getProduct(@PathVariable Long id) {
        return productService.getProduct(id);
    }

    @GetMapping("/api/v1/shopping-store")
    public Page<ProductDto> getProducts(
            @RequestParam(required = false) ProductCategory category,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "150") int size) {
        List<ProductDto> products = productService.getProducts(category);
        return new PageImpl<>(products, PageRequest.of(page, size), products.size());
    }

    @PostMapping("/api/v1/shopping-store")
    public ProductDto addProduct(@RequestBody ProductDto product) {
        return productService.addProduct(product);
    }

    @PutMapping("/api/v1/shopping-store")
    public ProductDto updateProduct(@RequestBody ProductDto product) {
        return productService.updateProduct(product);
    }

    @DeleteMapping("/api/v1/shopping-store/{id}")
    public void deactivateProduct(@PathVariable Long id) {
        productService.deactivateProduct(id);
    }

    @PutMapping("/api/v1/shopping-store/quantityState")
    public void updateQuantityState(@RequestParam Long productId, @RequestParam ProductAvailability quantityState) {
        productService.updateAvailability(productId, quantityState);
    }
}