package ru.yandex.practicum.controller;

import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.api.StoreApi;
import ru.yandex.practicum.dto.ProductDto;
import ru.yandex.practicum.enums.ProductAvailability;
import ru.yandex.practicum.enums.ProductCategory;
import ru.yandex.practicum.service.ProductService;

import java.util.List;

@RestController
@RequestMapping("/api/v1/shopping-store")
public class ProductController implements StoreApi {

    private final ProductService productService;

    public ProductController(ProductService productService) {
        this.productService = productService;
    }

    @Override
    @GetMapping("/products/{id}")
    public ProductDto getProduct(@PathVariable Long id) {
        return productService.getProduct(id);
    }

    @Override
    @GetMapping("/products")
    public List<ProductDto> getProducts(@RequestParam(required = false) ProductCategory category) {
        return productService.getProducts(category);
    }

    @Override
    @PostMapping("/products")
    public ProductDto addProduct(@RequestBody ProductDto product) {
        return productService.addProduct(product);
    }

    @Override
    @PutMapping("/products")
    public ProductDto updateProduct(@RequestBody ProductDto product) {
        return productService.updateProduct(product);
    }

    @Override
    @DeleteMapping("/products/{id}")
    public void deactivateProduct(@PathVariable Long id) {
        productService.deactivateProduct(id);
    }

    @Override
    @PutMapping("/quantityState")
    public void updateQuantityState(@RequestParam Long productId, @RequestParam ProductAvailability quantityState) {
        productService.updateAvailability(productId, quantityState);
    }
}