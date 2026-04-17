package ru.yandex.practicum.controller;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.dto.ProductDto;
import ru.yandex.practicum.enums.ProductCategory;
import ru.yandex.practicum.enums.ProductAvailability;
import ru.yandex.practicum.service.ProductService;

@RestController
@RequestMapping("/shopping-store")
public class ProductController {

    private final ProductService productService;

    public ProductController(ProductService productService) {
        this.productService = productService;
    }

    @GetMapping("/{id}")
    public ProductDto getProduct(@PathVariable Long id) {
        return productService.getProduct(id);
    }

    @GetMapping
    public Page<ProductDto> getProducts(
            @RequestParam(required = false) ProductCategory category,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "150") int size,
            @RequestParam(defaultValue = "productName,ASC") String sort) {

        String[] sortParts = sort.split(",");
        String sortField = sortParts[0];
        Sort.Direction direction = Sort.Direction.fromString(sortParts[1]);
        Pageable pageable = PageRequest.of(page, size, Sort.by(direction, sortField));

        return productService.getProducts(category, pageable);
    }

    @PutMapping
    public ProductDto addOrUpdateProduct(@RequestBody ProductDto product) {
        if (product.getId() == null) {
            return productService.addProduct(product);
        } else {
            return productService.updateProduct(product);
        }
    }

    @PostMapping
    public ProductDto addProductPost(@RequestBody ProductDto product) {
        return productService.addProduct(product);
    }

    @PostMapping("/removeProductFromStore")
    public void removeProductFromStore(@RequestBody Object body) {
        if (body instanceof Integer) {
            productService.deactivateProduct(((Integer) body).longValue());
        } else if (body instanceof Number) {
            productService.deactivateProduct(((Number) body).longValue());
        } else if (body instanceof java.util.Map) {
            java.util.Map<String, Object> map = (java.util.Map<String, Object>) body;
            Object productId = map.get("productId");
            if (productId instanceof Integer) {
                productService.deactivateProduct(((Integer) productId).longValue());
            } else if (productId instanceof Number) {
                productService.deactivateProduct(((Number) productId).longValue());
            }
        }
    }

    @PostMapping("/quantityState")
    public void setQuantityState(@RequestParam Long productId, @RequestParam ProductAvailability quantityState) {
        productService.updateQuantityState(productId, quantityState);
    }
}