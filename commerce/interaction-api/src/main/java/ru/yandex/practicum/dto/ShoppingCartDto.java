package ru.yandex.practicum.dto;

import java.util.Map;

public class ShoppingCartDto {
    private String shoppingCartId;
    private Map<String, Integer> products;

    public String getShoppingCartId() { return shoppingCartId; }
    public void setShoppingCartId(String shoppingCartId) { this.shoppingCartId = shoppingCartId; }

    public Map<String, Integer> getProducts() { return products; }
    public void setProducts(Map<String, Integer> products) { this.products = products; }
}