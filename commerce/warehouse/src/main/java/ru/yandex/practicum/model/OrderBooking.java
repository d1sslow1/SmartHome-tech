package ru.yandex.practicum.model;

import jakarta.persistence.*;
import java.util.Map;

@Entity
public class OrderBooking {

    @Id
    private String orderId;

    private String deliveryId;

    @ElementCollection
    @CollectionTable(name = "order_booking_products", joinColumns = @JoinColumn(name = "order_id"))
    @MapKeyColumn(name = "product_id")
    @Column(name = "quantity")
    private Map<String, Integer> products;

    public String getOrderId() { return orderId; }
    public void setOrderId(String orderId) { this.orderId = orderId; }

    public String getDeliveryId() { return deliveryId; }
    public void setDeliveryId(String deliveryId) { this.deliveryId = deliveryId; }

    public Map<String, Integer> getProducts() { return products; }
    public void setProducts(Map<String, Integer> products) { this.products = products; }
}