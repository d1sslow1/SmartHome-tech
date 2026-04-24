package ru.yandex.practicum.dto;

import java.util.Map;

public class ProductReturnRequest {
    private String orderId;
    private Map<String, Integer> products;

    public String getOrderId() { return orderId; }
    public void setOrderId(String orderId) { this.orderId = orderId; }

    public Map<String, Integer> getProducts() { return products; }
    public void setProducts(Map<String, Integer> products) { this.products = products; }
}