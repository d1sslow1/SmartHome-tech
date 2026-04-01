package ru.yandex.practicum.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.client.ShoppingStoreClient;
import ru.yandex.practicum.dto.ProductDto;
import ru.yandex.practicum.service.ProductService;

import java.util.List;
import java.util.UUID;

@Slf4j
@RestController
@RequestMapping("/api/v1/shopping-store")
@RequiredArgsConstructor
public class ProductController implements ShoppingStoreClient {

    private final ProductService productService;

    @GetMapping("/products")
    public List<ProductDto> getProducts() {
        log.info("GET /products");
        return productService.getAllActiveProducts();
    }

    @GetMapping("/products/{productId}")
    public ProductDto getProduct(@PathVariable UUID productId) {
        log.info("GET /products/{}", productId);
        return productService.getProductById(productId);
    }

    @PostMapping("/products")
    @ResponseStatus(HttpStatus.CREATED)
    public ProductDto createProduct(@RequestBody ProductDto productDto) {
        log.info("POST /products");
        return productService.createProduct(productDto);
    }

    @PutMapping("/products/{productId}")
    public ProductDto updateProduct(@PathVariable UUID productId, @RequestBody ProductDto productDto) {
        log.info("PUT /products/{}", productId);
        return productService.updateProduct(productId, productDto);
    }

    @DeleteMapping("/products/{productId}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void deleteProduct(@PathVariable UUID productId) {
        log.info("DELETE /products/{}", productId);
        productService.deleteProduct(productId);
    }

    @PostMapping("/products/{productId}/activate")
    public void activateProduct(@PathVariable UUID productId) {
        log.info("POST /products/{}/activate", productId);
        productService.activateProduct(productId);
    }

    @PostMapping("/products/{productId}/deactivate")
    public void deactivateProduct(@PathVariable UUID productId) {
        log.info("POST /products/{}/deactivate", productId);
        productService.deactivateProduct(productId);
    }

    @GetMapping("/products/category/{category}")
    public List<ProductDto> getProductsByCategory(@PathVariable String category) {
        log.info("GET /products/category/{}", category);
        return productService.getProductsByCategory(category);
    }
}