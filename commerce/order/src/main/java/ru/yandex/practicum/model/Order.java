package ru.yandex.practicum.model;

import jakarta.persistence.*;
import java.util.Map;

@Entity
@Table(name = "orders")
public class Order {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private String orderId;

    private String shoppingCartId;

    @ElementCollection
    @CollectionTable(name = "order_products", joinColumns = @JoinColumn(name = "order_id"))
    @MapKeyColumn(name = "product_id")
    @Column(name = "quantity")
    private Map<String, Integer> products;

    private String paymentId;
    private String deliveryId;
    private String state;
    private Double deliveryWeight;
    private Double deliveryVolume;
    private Boolean fragile;
    private Double totalPrice;
    private Double deliveryPrice;
    private Double productPrice;

    public String getOrderId() { return orderId; }
    public void setOrderId(String orderId) { this.orderId = orderId; }

    public String getShoppingCartId() { return shoppingCartId; }
    public void setShoppingCartId(String shoppingCartId) { this.shoppingCartId = shoppingCartId; }

    public Map<String, Integer> getProducts() { return products; }
    public void setProducts(Map<String, Integer> products) { this.products = products; }

    public String getPaymentId() { return paymentId; }
    public void setPaymentId(String paymentId) { this.paymentId = paymentId; }

    public String getDeliveryId() { return deliveryId; }
    public void setDeliveryId(String deliveryId) { this.deliveryId = deliveryId; }

    public String getState() { return state; }
    public void setState(String state) { this.state = state; }

    public Double getDeliveryWeight() { return deliveryWeight; }
    public void setDeliveryWeight(Double deliveryWeight) { this.deliveryWeight = deliveryWeight; }

    public Double getDeliveryVolume() { return deliveryVolume; }
    public void setDeliveryVolume(Double deliveryVolume) { this.deliveryVolume = deliveryVolume; }

    public Boolean getFragile() { return fragile; }
    public void setFragile(Boolean fragile) { this.fragile = fragile; }

    public Double getTotalPrice() { return totalPrice; }
    public void setTotalPrice(Double totalPrice) { this.totalPrice = totalPrice; }

    public Double getDeliveryPrice() { return deliveryPrice; }
    public void setDeliveryPrice(Double deliveryPrice) { this.deliveryPrice = deliveryPrice; }

    public Double getProductPrice() { return productPrice; }
    public void setProductPrice(Double productPrice) { this.productPrice = productPrice; }
}