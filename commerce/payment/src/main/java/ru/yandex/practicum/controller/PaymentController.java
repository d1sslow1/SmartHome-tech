package ru.yandex.practicum.controller;

import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.api.PaymentApi;
import ru.yandex.practicum.dto.OrderDto;
import ru.yandex.practicum.dto.PaymentDto;
import ru.yandex.practicum.service.PaymentService;

@RestController
public class PaymentController implements PaymentApi {

    private final PaymentService paymentService;

    public PaymentController(PaymentService paymentService) {
        this.paymentService = paymentService;
    }

    @Override
    public PaymentDto payment(@RequestBody OrderDto order) {
        return paymentService.payment(order);
    }

    @Override
    public Double getTotalCost(@RequestBody OrderDto order) {
        return paymentService.getTotalCost(order);
    }

    @Override
    public Double productCost(@RequestBody OrderDto order) {
        return paymentService.productCost(order);
    }

    @Override
    public void paymentSuccess(@RequestBody String paymentId) {
        paymentService.paymentSuccess(paymentId);
    }

    @Override
    public void paymentFailed(@RequestBody String paymentId) {
        paymentService.paymentFailed(paymentId);
    }
}