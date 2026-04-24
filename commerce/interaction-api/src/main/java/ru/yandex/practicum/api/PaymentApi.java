package ru.yandex.practicum.api;

import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import ru.yandex.practicum.dto.OrderDto;
import ru.yandex.practicum.dto.PaymentDto;

public interface PaymentApi {

    String BASE_PATH = "/payment";

    @PostMapping(BASE_PATH)
    PaymentDto payment(@RequestBody OrderDto order);

    @PostMapping(BASE_PATH + "/totalCost")
    Double getTotalCost(@RequestBody OrderDto order);

    @PostMapping(BASE_PATH + "/productCost")
    Double productCost(@RequestBody OrderDto order);

    @PostMapping(BASE_PATH + "/refund")
    void paymentSuccess(@RequestBody String paymentId);

    @PostMapping(BASE_PATH + "/failed")
    void paymentFailed(@RequestBody String paymentId);
}