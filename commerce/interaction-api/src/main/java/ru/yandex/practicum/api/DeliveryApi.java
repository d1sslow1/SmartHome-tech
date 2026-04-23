package ru.yandex.practicum.api;

import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.dto.DeliveryDto;
import ru.yandex.practicum.dto.OrderDto;

public interface DeliveryApi {

    @PutMapping("/delivery")
    DeliveryDto planDelivery(@RequestBody DeliveryDto delivery);

    @PostMapping("/delivery/successful")
    void deliverySuccessful(@RequestBody String orderId);

    @PostMapping("/delivery/picked")
    void deliveryPicked(@RequestBody String orderId);

    @PostMapping("/delivery/failed")
    void deliveryFailed(@RequestBody String orderId);

    @PostMapping("/delivery/cost")
    Double deliveryCost(@RequestBody OrderDto order);
}