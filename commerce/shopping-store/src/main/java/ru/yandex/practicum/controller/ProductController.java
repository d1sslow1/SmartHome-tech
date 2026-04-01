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

    @Override
    @GetMapping("/products")
    public List<ProductDto> getProducts() {
        log.info("GET /products");
        return productService.getAllActiveProducts();
    }

    @Override
    @GetMapping("/products/{productId}")
    public ProductDto getProduct(@PathVariable("productId") UUID productId) {
        log.info("GET /products/{}", productId);
        return productService.getProductById(productId);
    }

    @Override
    @PostMapping("/products")
    @ResponseStatus(HttpStatus.CREATED)
    public ProductDto createProduct(@RequestBody ProductDto productDto) {
        log.info("POST /products");
        return productService.createProduct(productDto);
    }

    @Override
    @PutMapping("/products/{productId}")
    public ProductDto updateProduct(@PathVariable("productId") UUID productId,
                                    @RequestBody ProductDto productDto) {
        log.info("PUT /products/{}", productId);
        return productService.updateProduct(productId, productDto);
    }

    @PutMapping
    public ProductDto updateProductWithoutId(@RequestBody ProductDto productDto) {
        log.info("PUT /api/v1/shopping-store - updating product");
        if (productDto.getProductId() == null) {
            throw new IllegalArgumentException("Product ID is required");
        }
        return productService.updateProduct(productDto.getProductId(), productDto);
    }

    @Override
    @DeleteMapping("/products/{productId}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void deleteProduct(@PathVariable("productId") UUID productId) {
        log.info("DELETE /products/{}", productId);
        productService.deleteProduct(productId);
    }

    @Override
    @PostMapping("/products/{productId}/activate")
    public void activateProduct(@PathVariable("productId") UUID productId) {
        log.info("POST /products/{}/activate", productId);
        productService.activateProduct(productId);
    }

    @Override
    @PostMapping("/products/{productId}/deactivate")
    public void deactivateProduct(@PathVariable("productId") UUID productId) {
        log.info("POST /products/{}/deactivate", productId);
        productService.deactivateProduct(productId);
    }

    @Override
    @GetMapping("/products/category/{category}")
    public List<ProductDto> getProductsByCategory(@PathVariable("category") String category) {
        log.info("GET /products/category/{}", category);
        return productService.getProductsByCategory(category);
    }
}