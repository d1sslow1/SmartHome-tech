package ru.yandex.practicum.service;

import ru.yandex.practicum.dto.OrderDto;
import ru.yandex.practicum.dto.PaymentDto;

public interface PaymentService {
    PaymentDto payment(OrderDto order);
    Double getTotalCost(OrderDto order);
    Double productCost(OrderDto order);
    void paymentSuccess(String paymentId);
    void paymentFailed(String paymentId);
}