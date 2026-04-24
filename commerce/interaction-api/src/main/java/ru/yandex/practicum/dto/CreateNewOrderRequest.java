package ru.yandex.practicum.dto;

public class CreateNewOrderRequest {
    private ShoppingCartDto shoppingCart;
    private AddressDto deliveryAddress;

    public ShoppingCartDto getShoppingCart() { return shoppingCart; }
    public void setShoppingCart(ShoppingCartDto shoppingCart) { this.shoppingCart = shoppingCart; }

    public AddressDto getDeliveryAddress() { return deliveryAddress; }
    public void setDeliveryAddress(AddressDto deliveryAddress) { this.deliveryAddress = deliveryAddress; }
}