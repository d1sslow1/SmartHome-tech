package ru.yandex.practicum.model;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.Setter;

import java.util.UUID;

@Entity
@Table(name = "warehouse_products")
@Getter
@Setter
public class WarehouseProduct {

    @Id
    private UUID productId;

    @Column(nullable = false)
    private Integer quantity;

    @Column(nullable = false)
    private Double width;

    @Column(nullable = false)
    private Double height;

    @Column(nullable = false)
    private Double depth;

    @Column(nullable = false)
    private Double weight;

    @Column(nullable = false)
    private Boolean fragile;
}