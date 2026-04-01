package ru.yandex.practicum.mapper;

import org.springframework.stereotype.Component;
import ru.yandex.practicum.dto.ProductDto;
import ru.yandex.practicum.model.Product;

@Component
public class ProductMapper {

    public ProductDto toDto(Product product) {
        if (product == null) return null;

        return ProductDto.builder()
                .productId(product.getId())
                .productName(product.getName())
                .description(product.getDescription())
                .price(product.getPrice())
                .quantityState(product.getQuantityState())
                .productState(product.getState())
                .productCategory(product.getCategory())
                .imageSrc(product.getImageUrl())
                .build();
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