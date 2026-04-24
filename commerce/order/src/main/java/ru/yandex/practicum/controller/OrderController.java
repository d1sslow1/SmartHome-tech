package ru.yandex.practicum.controller;

import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.api.OrderApi;
import ru.yandex.practicum.dto.*;
import ru.yandex.practicum.service.OrderService;

import java.util.List;

@RestController
public class OrderController implements OrderApi {

    private final OrderService orderService;

    public OrderController(OrderService orderService) {
        this.orderService = orderService;
    }

    @Override
    public List<OrderDto> getClientOrders(@RequestParam String username) {
        return orderService.getClientOrders(username);
    }

    @Override
    public OrderDto createNewOrder(@RequestBody CreateNewOrderRequest request) {
        return orderService.createNewOrder(request);
    }

    @Override
    public OrderDto productReturn(@RequestBody ProductReturnRequest request) {
        return orderService.productReturn(request);
    }

    @Override
    public OrderDto payment(@RequestBody String orderId) {
        return orderService.payment(orderId);
    }

    @Override
    public OrderDto paymentFailed(@RequestBody String orderId) {
        return orderService.paymentFailed(orderId);
    }

    @Override
    public OrderDto delivery(@RequestBody String orderId) {
        return orderService.delivery(orderId);
    }

    @Override
    public OrderDto deliveryFailed(@RequestBody String orderId) {
        return orderService.deliveryFailed(orderId);
    }

    @Override
    public OrderDto complete(@RequestBody String orderId) {
        return orderService.complete(orderId);
    }

    @Override
    public OrderDto calculateTotalCost(@RequestBody String orderId) {
        return orderService.calculateTotalCost(orderId);
    }

    @Override
    public OrderDto calculateDeliveryCost(@RequestBody String orderId) {
        return orderService.calculateDeliveryCost(orderId);
    }

    @Override
    public OrderDto assembly(@RequestBody String orderId) {
        return orderService.assembly(orderId);
    }

    @Override
    public OrderDto assemblyFailed(@RequestBody String orderId) {
        return orderService.assemblyFailed(orderId);
    }
}