package ru.yandex.practicum.dto;

import ru.yandex.practicum.enums.ProductAvailability;
import ru.yandex.practicum.enums.ProductCategory;
import ru.yandex.practicum.enums.ProductStatus;

import java.util.List;

public class ProductDto {
    private Long id;

    @com.fasterxml.jackson.annotation.JsonProperty("productName")
    private String name;

    private String description;

    @com.fasterxml.jackson.annotation.JsonProperty("productCategory")
    private ProductCategory category;

    @com.fasterxml.jackson.annotation.JsonProperty("quantityState")
    private ProductAvailability availability;

    @com.fasterxml.jackson.annotation.JsonProperty("productState")
    private ProductStatus status;

    @com.fasterxml.jackson.annotation.JsonProperty("imageSrc")
    private List<String> images;

    private Double price;  // Добавляем price

    public Long getId() { return id; }
    public void setId(Long id) { this.id = id; }

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }

    public String getDescription() { return description; }
    public void setDescription(String description) { this.description = description; }

    public ProductCategory getCategory() { return category; }
    public void setCategory(ProductCategory category) { this.category = category; }

    public ProductAvailability getAvailability() { return availability; }
    public void setAvailability(ProductAvailability availability) { this.availability = availability; }

    public ProductStatus getStatus() { return status; }
    public void setStatus(ProductStatus status) { this.status = status; }

    public List<String> getImages() { return images; }
    public void setImages(List<String> images) { this.images = images; }

    public Double getPrice() { return price; }
    public void setPrice(Double price) { this.price = price; }
}