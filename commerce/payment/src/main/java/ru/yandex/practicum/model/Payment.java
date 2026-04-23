package ru.yandex.practicum.model;

import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;

@Entity
public class Payment {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private String paymentId;

    private String orderId;
    private Double totalPayment;
    private Double deliveryTotal;
    private Double feeTotal;
    private String state; 

    public String getPaymentId() { return paymentId; }
    public void setPaymentId(String paymentId) { this.paymentId = paymentId; }

    public String getOrderId() { return orderId; }
    public void setOrderId(String orderId) { this.orderId = orderId; }

    public Double getTotalPayment() { return totalPayment; }
    public void setTotalPayment(Double totalPayment) { this.totalPayment = totalPayment; }

    public Double getDeliveryTotal() { return deliveryTotal; }
    public void setDeliveryTotal(Double deliveryTotal) { this.deliveryTotal = deliveryTotal; }

    public Double getFeeTotal() { return feeTotal; }
    public void setFeeTotal(Double feeTotal) { this.feeTotal = feeTotal; }

    public String getState() { return state; }
    public void setState(String state) { this.state = state; }
}