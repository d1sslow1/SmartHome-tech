package ru.yandex.practicum.dto;

public class WarehouseItemDto {
    private String productId;
    private int quantity;
    private double weight;
    private DimensionDto dimension;
    private boolean fragile;

    public String getProductId() { return productId; }
    public void setProductId(String productId) { this.productId = productId; }
    public int getQuantity() { return quantity; }
    public void setQuantity(int quantity) { this.quantity = quantity; }
    public double getWeight() { return weight; }
    public void setWeight(double weight) { this.weight = weight; }
    public DimensionDto getDimension() { return dimension; }
    public void setDimension(DimensionDto dimension) { this.dimension = dimension; }
    public boolean isFragile() { return fragile; }
    public void setFragile(boolean fragile) { this.fragile = fragile; }
}