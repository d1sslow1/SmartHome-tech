package ru.yandex.practicum.dto;

public class ShippedToDeliveryRequest {
    private String orderId;
    private String deliveryId;

    public String getOrderId() { return orderId; }
    public void setOrderId(String orderId) { this.orderId = orderId; }

    public String getDeliveryId() { return deliveryId; }
    public void setDeliveryId(String deliveryId) { this.deliveryId = deliveryId; }
}