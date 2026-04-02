package ru.yandex.practicum.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.dto.ProductDto;
import ru.yandex.practicum.dto.ProductState;
import ru.yandex.practicum.service.ProductService;

import java.util.List;
import java.util.UUID;

@Slf4j
@RestController
@RequestMapping("/api/v1/shopping-store")
@RequiredArgsConstructor
public class ProductController {

    private final ProductService productService;

    @GetMapping("/products")
    public Page<ProductDto> getProducts(
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "10") int size) {
        log.info("GET /products - page={}, size={}", page, size);
        Pageable pageable = PageRequest.of(page, size, Sort.by("name").ascending());
        return productService.getProductsPage(pageable);
    }

    @GetMapping("/products/{productId}")
    public ProductDto getProduct(@PathVariable("productId") UUID productId) {
        log.info("GET /products/{}", productId);
        return productService.getProductById(productId);
    }

    @GetMapping("/{productId}")
    public ProductDto getProductByIdDirect(@PathVariable("productId") UUID productId) {
        log.info("GET /{}", productId);
        return productService.getProductById(productId);
    }

    @PostMapping("/products")
    @ResponseStatus(HttpStatus.CREATED)
    public ProductDto createProduct(@RequestBody ProductDto productDto) {
        log.info("POST /products");
        if (productDto.getProductState() == null) {
            productDto.setProductState(ProductState.ACTIVE);
        }
        return productService.createProduct(productDto);
    }

    @PutMapping("/products/{productId}")
    public ProductDto updateProduct(@PathVariable("productId") UUID productId,
                                    @RequestBody ProductDto productDto) {
        log.info("PUT /products/{}", productId);
        return productService.updateProduct(productId, productDto);
    }

    @DeleteMapping("/products/{productId}")
    @ResponseStatus(HttpStatus.OK)
    public ProductDto deleteProduct(@PathVariable("productId") UUID productId) {
        log.info("DELETE /products/{}", productId);
        ProductDto result = productService.deleteProduct(productId);
        log.info("DELETE result - productState: {}", result.getProductState());
        return result;
    }

    @DeleteMapping("/{productId}")
    @ResponseStatus(HttpStatus.OK)
    public ProductDto deleteProductByIdDirect(@PathVariable("productId") UUID productId) {
        log.info("DELETE /{}", productId);
        ProductDto result = productService.deleteProduct(productId);
        log.info("DELETE result - productState: {}", result.getProductState());
        return result;
    }

    @PostMapping("/products/{productId}/activate")
    @ResponseStatus(HttpStatus.OK)
    public void activateProduct(@PathVariable("productId") UUID productId) {
        log.info("POST /products/{}/activate", productId);
        productService.activateProduct(productId);
    }

    @PostMapping("/products/{productId}/deactivate")
    @ResponseStatus(HttpStatus.OK)
    public void deactivateProduct(@PathVariable("productId") UUID productId) {
        log.info("POST /products/{}/deactivate", productId);
        productService.deactivateProduct(productId);
    }

    @GetMapping("/products/category/{category}")
    public List<ProductDto> getProductsByCategory(@PathVariable("category") String category) {
        log.info("GET /products/category/{}", category);
        return productService.getProductsByCategory(category);
    }

    @PostMapping("/quantityState")
    @ResponseStatus(HttpStatus.OK)
    public ProductDto updateQuantityState(@RequestParam UUID productId,
                                          @RequestParam String quantityState) {
        log.info("POST /quantityState - productId={}, quantityState={}", productId, quantityState);
        if (quantityState == null || quantityState.trim().isEmpty()) {
            throw new IllegalArgumentException("quantityState cannot be empty");
        }
        return productService.updateQuantityState(productId, quantityState);
    }

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public ProductDto createProductRoot(@RequestBody ProductDto productDto) {
        log.info("POST /api/v1/shopping-store");
        if (productDto.getProductState() == null) {
            productDto.setProductState(ProductState.ACTIVE);
        }
        return productService.createProduct(productDto);
    }

    @PutMapping
    public ProductDto updateProductRoot(@RequestBody ProductDto productDto) {
        log.info("PUT /api/v1/shopping-store");
        UUID productId = productDto.getProductId();
        if (productId == null) {
            return productService.createProduct(productDto);
        }
        return productService.updateProduct(productId, productDto);
    }

    @DeleteMapping
    @ResponseStatus(HttpStatus.OK)
    public ProductDto deleteProductRoot(@RequestParam UUID productId) {
        log.info("DELETE /api/v1/shopping-store?productId={}", productId);
        ProductDto result = productService.deleteProduct(productId);
        log.info("DELETE result - productState: {}", result.getProductState());
        return result;
    }

    @GetMapping
    public List<ProductDto> getProductsRoot(@RequestParam(required = false) String category) {
        log.info("GET /api/v1/shopping-store?category={}", category);
        if (category != null && !category.isEmpty()) {
            return productService.getProductsByCategory(category);
        }
        return productService.getAllActiveProducts();
    }
    @PostMapping("/removeProductFromStore")
    @ResponseStatus(HttpStatus.OK)
    public ProductDto removeProductFromStore(@RequestBody String productId) {
        log.info("POST /removeProductFromStore - productId={}", productId);
        productId = productId.replace("\"", "");
        return productService.deleteProduct(UUID.fromString(productId));
    }

}