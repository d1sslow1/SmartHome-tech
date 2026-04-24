package ru.yandex.practicum.dto;

public class PaymentDto {
    private String paymentId;
    private Double totalPayment;
    private Double deliveryTotal;
    private Double feeTotal;

    public String getPaymentId() { return paymentId; }
    public void setPaymentId(String paymentId) { this.paymentId = paymentId; }

    public Double getTotalPayment() { return totalPayment; }
    public void setTotalPayment(Double totalPayment) { this.totalPayment = totalPayment; }

    public Double getDeliveryTotal() { return deliveryTotal; }
    public void setDeliveryTotal(Double deliveryTotal) { this.deliveryTotal = deliveryTotal; }

    public Double getFeeTotal() { return feeTotal; }
    public void setFeeTotal(Double feeTotal) { this.feeTotal = feeTotal; }
}