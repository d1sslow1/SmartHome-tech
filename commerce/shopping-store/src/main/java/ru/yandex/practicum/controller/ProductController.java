package ru.yandex.practicum.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.client.ShoppingStoreClient;
import ru.yandex.practicum.dto.ProductDto;
import ru.yandex.practicum.service.ProductService;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Slf4j
@RestController
@RequestMapping("/api/v1/shopping-store")
@RequiredArgsConstructor
public class ProductController implements ShoppingStoreClient {

    private final ProductService productService;

    @Override
    @GetMapping("/products")
    public Map<String, Object> getProducts() {
        log.info("GET /products");
        List<ProductDto> products = productService.getAllActiveProducts();
        Map<String, Object> response = new HashMap<>();
        response.put("content", products);
        response.put("totalElements", products.size());
        response.put("totalPages", 1);
        response.put("page", 0);
        response.put("size", products.size());
        return response;
    }

    @GetMapping(value = "/products", params = {"page", "size"})
    public Map<String, Object> getProductsWithPagination(
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "20") int size,
            @RequestParam(defaultValue = "name") String sort) {
        log.info("GET /products with pagination");

        Pageable pageable = PageRequest.of(page, size, Sort.by(sort).descending());
        Page<ProductDto> productPage = productService.getProductsPage(pageable);

        Map<String, Object> response = new HashMap<>();
        response.put("content", productPage.getContent());
        response.put("totalElements", productPage.getTotalElements());
        response.put("totalPages", productPage.getTotalPages());
        response.put("page", productPage.getNumber());
        response.put("size", productPage.getSize());

        return response;
    }

    @Override
    @GetMapping("/products/{productId}")
    public ProductDto getProduct(@PathVariable("productId") UUID productId) {
        log.info("GET /products/{}", productId);
        return productService.getProductById(productId);
    }

    @GetMapping("/{productId}")
    public ProductDto getProductByIdPath(@PathVariable("productId") UUID productId) {
        log.info("GET /{}", productId);
        return productService.getProductById(productId);
    }

    @Override
    @PostMapping("/products")
    @ResponseStatus(HttpStatus.CREATED)
    public ProductDto createProduct(@RequestBody ProductDto productDto) {
        log.info("POST /products");
        if (productDto.getProductState() == null) {
            productDto.setProductState(ru.yandex.practicum.dto.ProductState.ACTIVE);
        }
        return productService.createProduct(productDto);
    }

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public ProductDto createProductDirect(@RequestBody ProductDto productDto) {
        log.info("POST /api/v1/shopping-store");
        if (productDto.getProductState() == null) {
            productDto.setProductState(ru.yandex.practicum.dto.ProductState.ACTIVE);
        }
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
        log.info("PUT /api/v1/shopping-store");
        if (productDto.getProductId() != null) {
            return productService.updateProduct(productDto.getProductId(), productDto);
        }
        if (productDto.getProductState() == null) {
            productDto.setProductState(ru.yandex.practicum.dto.ProductState.ACTIVE);
        }
        return productService.createProduct(productDto);
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
    public Map<String, Object> getProductsByCategory(@PathVariable("category") String category) {
        log.info("GET /products/category/{}", category);
        List<ProductDto> products = productService.getProductsByCategory(category);
        Map<String, Object> response = new HashMap<>();
        response.put("content", products);
        response.put("totalElements", products.size());
        response.put("totalPages", 1);
        response.put("page", 0);
        response.put("size", products.size());
        return response;
    }

    @GetMapping
    public Map<String, Object> getProductsByCategoryParam(
            @RequestParam(required = false) String category,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "20") int size) {
        log.info("GET /api/v1/shopping-store?category={}, page={}, size={}", category, page, size);

        Pageable pageable = PageRequest.of(page, size);
        Page<ProductDto> productPage;

        if (category != null && !category.isEmpty()) {
            productPage = productService.getProductsByCategoryPage(category, pageable);
        } else {
            productPage = productService.getProductsPage(pageable);
        }

        Map<String, Object> response = new HashMap<>();
        response.put("content", productPage.getContent());
        response.put("totalElements", productPage.getTotalElements());
        response.put("totalPages", productPage.getTotalPages());
        response.put("page", productPage.getNumber());
        response.put("size", productPage.getSize());

        return response;
    }

    @PostMapping("/removeProductFromStore")
    @ResponseStatus(HttpStatus.OK)
    public void removeProductFromStore(@RequestBody String productId) {
        log.info("POST /removeProductFromStore - productId={}", productId);
        productId = productId.replace("\"", "");
        productService.deleteProduct(UUID.fromString(productId));
    }

    @PostMapping("/quantityState")
    @ResponseStatus(HttpStatus.OK)
    public void updateQuantityState(@RequestParam UUID productId,
                                    @RequestParam String quantityState) {
        log.info("POST /quantityState - productId={}, quantityState={}", productId, quantityState);
        productService.updateQuantityState(productId, quantityState);
    }
}