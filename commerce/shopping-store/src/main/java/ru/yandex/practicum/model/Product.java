package ru.yandex.practicum.model;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.Setter;
import ru.yandex.practicum.dto.ProductCategory;
import ru.yandex.practicum.dto.ProductQuantityState;
import ru.yandex.practicum.dto.ProductState;

import java.math.BigDecimal;
import java.util.UUID;

@Entity
@Table(name = "products")
@Getter
@Setter
public class Product {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID id;

    @Column(nullable = false)
    private String name;

    @Column(length = 1000)
    private String description;

    @Column(nullable = false, precision = 10, scale = 2)
    private BigDecimal price;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private ProductQuantityState quantityState;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private ProductState state;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private ProductCategory category;

    private String imageUrl;
}