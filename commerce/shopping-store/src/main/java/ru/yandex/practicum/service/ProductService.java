package ru.yandex.practicum.service;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import ru.yandex.practicum.dto.ProductDto;
import ru.yandex.practicum.enums.ProductAvailability;
import ru.yandex.practicum.enums.ProductCategory;

public interface ProductService {
    Page<ProductDto> getProducts(ProductCategory category, Pageable pageable);
    ProductDto getProduct(Long id);
    ProductDto addProduct(ProductDto product);
    ProductDto updateProduct(ProductDto product);
    void deactivateProduct(Long id);
    void updateQuantityState(Long id, ProductAvailability availability);
}