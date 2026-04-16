package ru.yandex.practicum.controller;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.dto.ProductDto;
import ru.yandex.practicum.enums.ProductCategory;
import ru.yandex.practicum.enums.ProductAvailability;
import ru.yandex.practicum.service.ProductService;

import java.util.List;

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
            @RequestParam(defaultValue = "150") int size) {
        return productService.getProducts(category, PageRequest.of(page, size));
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
    public void removeProductFromStore(@RequestBody ProductDto product) {
        productService.deactivateProduct(product.getId());
    }

    @PostMapping("/quantityState")
    public void setQuantityState(@RequestParam Long productId, @RequestParam ProductAvailability quantityState) {
        productService.updateQuantityState(productId, quantityState);
    }
}