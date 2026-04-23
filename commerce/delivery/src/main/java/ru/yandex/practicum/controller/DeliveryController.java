package ru.yandex.practicum.controller;

import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.api.DeliveryApi;
import ru.yandex.practicum.dto.DeliveryDto;
import ru.yandex.practicum.dto.OrderDto;
import ru.yandex.practicum.service.DeliveryService;

@RestController
public class DeliveryController implements DeliveryApi {

    private final DeliveryService deliveryService;

    public DeliveryController(DeliveryService deliveryService) {
        this.deliveryService = deliveryService;
    }

    @Override
    public DeliveryDto planDelivery(@RequestBody DeliveryDto delivery) {
        return deliveryService.planDelivery(delivery);
    }

    @Override
    public void deliverySuccessful(@RequestBody String orderId) {
        deliveryService.deliverySuccessful(orderId);
    }

    @Override
    public void deliveryPicked(@RequestBody String orderId) {
        deliveryService.deliveryPicked(orderId);
    }

    @Override
    public void deliveryFailed(@RequestBody String orderId) {
        deliveryService.deliveryFailed(orderId);
    }

    @Override
    public Double deliveryCost(@RequestBody OrderDto order) {
        return deliveryService.deliveryCost(order);
    }
}