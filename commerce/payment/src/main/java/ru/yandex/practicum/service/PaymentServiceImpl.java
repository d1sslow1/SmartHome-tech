package ru.yandex.practicum.service;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.client.OrderClient;
import ru.yandex.practicum.client.ShoppingStoreClient;
import ru.yandex.practicum.dto.OrderDto;
import ru.yandex.practicum.dto.PaymentDto;
import ru.yandex.practicum.dto.ProductDto;
import ru.yandex.practicum.model.Payment;
import ru.yandex.practicum.repository.PaymentRepository;

import java.util.Map;

@Service
@Transactional
public class PaymentServiceImpl implements PaymentService {

    private final PaymentRepository paymentRepository;
    private final ShoppingStoreClient shoppingStoreClient;
    private final OrderClient orderClient;

    public PaymentServiceImpl(PaymentRepository paymentRepository,
                              ShoppingStoreClient shoppingStoreClient,
                              OrderClient orderClient) {
        this.paymentRepository = paymentRepository;
        this.shoppingStoreClient = shoppingStoreClient;
        this.orderClient = orderClient;
    }

    @Override
    public Double productCost(OrderDto order) {
        Map<String, Integer> products = order.getProducts();
        double total = 0.0;

        for (Map.Entry<String, Integer> entry : products.entrySet()) {
            try {
                Long productId = Long.parseLong(entry.getKey());
                ProductDto product = shoppingStoreClient.getProduct(productId);
                total += product.getPrice() * entry.getValue();
            } catch (NumberFormatException e) {

            }
        }

        return total;
    }

    @Override
    public Double getTotalCost(OrderDto order) {
        double productCost = productCost(order);
        double deliveryCost = order.getDeliveryPrice() != null ? order.getDeliveryPrice() : 0.0;
        double tax = productCost * 0.1; // НДС 10%
        return productCost + deliveryCost + tax;
    }

    @Override
    public PaymentDto payment(OrderDto order) {
        Payment payment = paymentRepository.findByOrderId(order.getOrderId())
                .orElse(new Payment());

        double productCost = productCost(order);
        double deliveryCost = order.getDeliveryPrice() != null ? order.getDeliveryPrice() : 0.0;
        double tax = productCost * 0.1;
        double totalPayment = productCost + deliveryCost + tax;

        payment.setOrderId(order.getOrderId());
        payment.setTotalPayment(totalPayment);
        payment.setDeliveryTotal(deliveryCost);
        payment.setFeeTotal(tax);
        payment.setState("PENDING");

        payment = paymentRepository.save(payment);

        PaymentDto dto = new PaymentDto();
        dto.setPaymentId(payment.getPaymentId());
        dto.setTotalPayment(payment.getTotalPayment());
        dto.setDeliveryTotal(payment.getDeliveryTotal());
        dto.setFeeTotal(payment.getFeeTotal());

        return dto;
    }

    @Override
    public void paymentSuccess(String paymentId) {
        Payment payment = paymentRepository.findById(paymentId)
                .orElseThrow(() -> new RuntimeException("Payment not found"));
        payment.setState("SUCCESS");
        paymentRepository.save(payment);

        orderClient.payment(payment.getOrderId());
    }

    @Override
    public void paymentFailed(String paymentId) {
        Payment payment = paymentRepository.findById(paymentId)
                .orElseThrow(() -> new RuntimeException("Payment not found"));
        payment.setState("FAILED");
        paymentRepository.save(payment);

        orderClient.paymentFailed(payment.getOrderId());
    }
}