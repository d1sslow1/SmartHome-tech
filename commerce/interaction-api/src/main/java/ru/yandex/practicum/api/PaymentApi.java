package ru.yandex.practicum.api;

import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import ru.yandex.practicum.dto.OrderDto;
import ru.yandex.practicum.dto.PaymentDto;

public interface PaymentApi {

    @PostMapping("/payment")
    PaymentDto payment(@RequestBody OrderDto order);

    @PostMapping("/payment/totalCost")
    Double getTotalCost(@RequestBody OrderDto order);

    @PostMapping("/payment/productCost")
    Double productCost(@RequestBody OrderDto order);

    @PostMapping("/payment/refund")
    void paymentSuccess(@RequestBody String paymentId);

    @PostMapping("/payment/failed")
    void paymentFailed(@RequestBody String paymentId);
}