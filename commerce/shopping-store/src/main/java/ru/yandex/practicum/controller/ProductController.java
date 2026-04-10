package ru.yandex.practicum.controller;

import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.api.StoreApi;
import ru.yandex.practicum.dto.ProductDto;
import ru.yandex.practicum.enums.ProductAvailability;
import ru.yandex.practicum.enums.ProductCategory;
import ru.yandex.practicum.service.ProductService;

import java.util.List;

@RestController
public class ProductController implements StoreApi {

    private final ProductService productService;

    public ProductController(ProductService productService) {
        this.productService = productService;
    }

    @Override
    public ProductDto getProduct(Long id) {
        return productService.getProduct(id);
    }

    @Override
    public List<ProductDto> getProducts(ProductCategory category) {
        return productService.getProducts(category);
    }

    @Override
    public ProductDto addProduct(ProductDto product) {
        return productService.addProduct(product);
    }

    @Override
    public ProductDto updateProduct(Long id, ProductDto product) {
        product.setId(id);
        return productService.updateProduct(product);
    }

    @Override
    public void deactivateProduct(Long id) {
        productService.deactivateProduct(id);
    }

    @Override
    public void updateQuantityState(Long id, ProductAvailability quantityState) {
        productService.updateAvailability(id, quantityState);
    }
}