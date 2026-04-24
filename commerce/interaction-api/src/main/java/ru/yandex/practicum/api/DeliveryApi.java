package ru.yandex.practicum.api;

import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.dto.DeliveryDto;
import ru.yandex.practicum.dto.OrderDto;

public interface DeliveryApi {

    String BASE_PATH = "/delivery";

    @PutMapping(BASE_PATH)
    DeliveryDto planDelivery(@RequestBody DeliveryDto delivery);

    @PostMapping(BASE_PATH + "/successful")
    void deliverySuccessful(@RequestBody String orderId);

    @PostMapping(BASE_PATH + "/picked")
    void deliveryPicked(@RequestBody String orderId);

    @PostMapping(BASE_PATH + "/failed")
    void deliveryFailed(@RequestBody String orderId);

    @PostMapping(BASE_PATH + "/cost")
    Double deliveryCost(@RequestBody OrderDto order);
}