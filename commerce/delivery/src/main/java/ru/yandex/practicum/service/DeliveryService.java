package ru.yandex.practicum.service;

import ru.yandex.practicum.dto.DeliveryDto;
import ru.yandex.practicum.dto.OrderDto;

public interface DeliveryService {
    DeliveryDto planDelivery(DeliveryDto delivery);
    void deliverySuccessful(String orderId);
    void deliveryPicked(String orderId);
    void deliveryFailed(String orderId);
    Double deliveryCost(OrderDto order);
}