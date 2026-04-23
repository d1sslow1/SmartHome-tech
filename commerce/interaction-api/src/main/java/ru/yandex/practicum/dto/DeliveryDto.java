package ru.yandex.practicum.dto;

public class DeliveryDto {
    private String deliveryId;
    private AddressDto fromAddress;
    private AddressDto toAddress;
    private String orderId;
    private String deliveryState;

    public String getDeliveryId() { return deliveryId; }
    public void setDeliveryId(String deliveryId) { this.deliveryId = deliveryId; }

    public AddressDto getFromAddress() { return fromAddress; }
    public void setFromAddress(AddressDto fromAddress) { this.fromAddress = fromAddress; }

    public AddressDto getToAddress() { return toAddress; }
    public void setToAddress(AddressDto toAddress) { this.toAddress = toAddress; }

    public String getOrderId() { return orderId; }
    public void setOrderId(String orderId) { this.orderId = orderId; }

    public String getDeliveryState() { return deliveryState; }
    public void setDeliveryState(String deliveryState) { this.deliveryState = deliveryState; }
}