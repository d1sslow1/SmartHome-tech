package ru.yandex.practicum.dto;

public class NewProductInWarehouseRequest {
    private String productId;
    private Boolean fragile;
    private DimensionDto dimension;
    private Double weight;

    public String getProductId() { return productId; }
    public void setProductId(String productId) { this.productId = productId; }

    public Boolean getFragile() { return fragile; }
    public void setFragile(Boolean fragile) { this.fragile = fragile; }

    public DimensionDto getDimension() { return dimension; }
    public void setDimension(DimensionDto dimension) { this.dimension = dimension; }

    public Double getWeight() { return weight; }
    public void setWeight(Double weight) { this.weight = weight; }
}