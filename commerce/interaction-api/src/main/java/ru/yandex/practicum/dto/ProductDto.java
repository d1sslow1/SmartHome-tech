package ru.yandex.practicum.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import ru.yandex.practicum.enums.ProductAvailability;
import ru.yandex.practicum.enums.ProductCategory;
import ru.yandex.practicum.enums.ProductStatus;

import java.util.List;

public class ProductDto {

    @JsonProperty("productId")
    private Long id;

    @JsonProperty("productName")
    private String name;

    private String description;

    @JsonProperty("productCategory")
    private ProductCategory category;

    @JsonProperty("quantityState")
    private ProductAvailability availability;

    @JsonProperty("productState")
    private ProductStatus status;

    @JsonProperty("imageSrc")
    private String imageSrc;  // Тесты ждут String, а не List!

    private Double price;

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

    public String getImageSrc() { return imageSrc; }
    public void setImageSrc(String imageSrc) { this.imageSrc = imageSrc; }

    public Double getPrice() { return price; }
    public void setPrice(Double price) { this.price = price; }
}