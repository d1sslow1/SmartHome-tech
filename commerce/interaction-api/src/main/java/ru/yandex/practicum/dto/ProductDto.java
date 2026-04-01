package ru.yandex.practicum.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.util.UUID;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ProductDto {
    private UUID productId;
    private String productName;
    private String description;
    private BigDecimal price;
    private ProductQuantityState quantityState;
    private ProductState productState;
    private ProductCategory productCategory;
    private String imageSrc;
}