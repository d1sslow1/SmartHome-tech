package ru.yandex.practicum.dto;

import java.util.Map;

public class AssemblyProductsForOrderRequest {
    private Map<String, Integer> products;
    private String orderId;

    public Map<String, Integer> getProducts() { return products; }
    public void setProducts(Map<String, Integer> products) { this.products = products; }

    public String getOrderId() { return orderId; }
    public void setOrderId(String orderId) { this.orderId = orderId; }
}