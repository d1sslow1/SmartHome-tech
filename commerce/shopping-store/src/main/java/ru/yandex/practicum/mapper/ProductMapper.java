package ru.yandex.practicum.mapper;

import org.springframework.stereotype.Component;
import ru.yandex.practicum.dto.ProductDto;
import ru.yandex.practicum.model.Product;

@Component
public class ProductMapper {

    public ProductDto toDto(Product product) {
        if (product == null) return null;

        ProductDto dto = new ProductDto();
        dto.setProductId(product.getId());
        dto.setProductName(product.getName());
        dto.setDescription(product.getDescription());
        dto.setPrice(product.getPrice());
        dto.setQuantityState(product.getQuantityState());
        dto.setProductState(product.getState());
        dto.setProductCategory(product.getCategory());
        dto.setImageSrc(product.getImageUrl());
        return dto;
    }

    public Product toEntity(ProductDto dto) {
        if (dto == null) return null;

        Product product = new Product();
        product.setId(dto.getProductId());
        product.setName(dto.getProductName());
        product.setDescription(dto.getDescription());
        product.setPrice(dto.getPrice());
        product.setQuantityState(dto.getQuantityState());
        product.setState(dto.getProductState());
        product.setCategory(dto.getProductCategory());
        product.setImageUrl(dto.getImageSrc());
        return product;
    }

    public void updateEntity(ProductDto dto, Product product) {
        if (dto.getProductName() != null) product.setName(dto.getProductName());
        if (dto.getDescription() != null) product.setDescription(dto.getDescription());
        if (dto.getPrice() != null) product.setPrice(dto.getPrice());
        if (dto.getQuantityState() != null) product.setQuantityState(dto.getQuantityState());
        if (dto.getProductCategory() != null) product.setCategory(dto.getProductCategory());
        if (dto.getImageSrc() != null) product.setImageUrl(dto.getImageSrc());
    }
}