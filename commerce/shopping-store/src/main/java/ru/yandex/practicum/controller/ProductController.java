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
    public ProductDto getProduct(@PathVariable String id) {
        if ("null".equals(id) || id == null || id.isEmpty()) {
            throw new RuntimeException("Invalid product id: " + id);
        }
        try {
            return productService.getProduct(Long.parseLong(id));
        } catch (NumberFormatException e) {
            return productService.getProductByStringId(id);
        }
    }

    @GetMapping("/api/v1/shopping-store")
    public Page<ProductDto> getProducts(
            @RequestParam(required = false) ProductCategory category,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "150") int size) {
        List<ProductDto> products = productService.getProducts(category);
        int start = (int) PageRequest.of(page, size).getOffset();
        int end = Math.min(start + size, products.size());
        List<ProductDto> pageContent = products.subList(start, end);
        return new PageImpl<>(pageContent, PageRequest.of(page, size), products.size());
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
    public void deactivateProduct(@PathVariable String id) {
        if ("null".equals(id) || id == null || id.isEmpty()) {
            throw new RuntimeException("Invalid product id: " + id);
        }
        try {
            productService.deactivateProduct(Long.parseLong(id));
        } catch (NumberFormatException e) {
            productService.deactivateProductByStringId(id);
        }
    }

    @PostMapping("/api/v1/shopping-store/removeProductFromStore")
    public void removeProductFromStore(@RequestParam String productId) {
        if ("null".equals(productId) || productId == null || productId.isEmpty()) {
            throw new RuntimeException("Invalid product id: " + productId);
        }
        try {
            productService.deactivateProduct(Long.parseLong(productId));
        } catch (NumberFormatException e) {
            productService.deactivateProductByStringId(productId);
        }
    }

    @PostMapping("/api/v1/shopping-store/quantityState")
    public void updateQuantityState(@RequestParam String productId, @RequestParam ProductAvailability quantityState) {
        if ("null".equals(productId) || productId == null || productId.isEmpty()) {
            throw new RuntimeException("Invalid product id: " + productId);
        }
        try {
            productService.updateAvailability(Long.parseLong(productId), quantityState);
        } catch (NumberFormatException e) {
            productService.updateAvailabilityByStringId(productId, quantityState);
        }
    }
}