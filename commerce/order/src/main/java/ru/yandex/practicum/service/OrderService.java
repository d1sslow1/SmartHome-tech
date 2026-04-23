package ru.yandex.practicum.service;

import ru.yandex.practicum.dto.*;

import java.util.List;

public interface OrderService {
    List<OrderDto> getClientOrders(String username);
    OrderDto createNewOrder(CreateNewOrderRequest request);
    OrderDto productReturn(ProductReturnRequest request);
    OrderDto payment(String orderId);
    OrderDto paymentFailed(String orderId);
    OrderDto delivery(String orderId);
    OrderDto deliveryFailed(String orderId);
    OrderDto complete(String orderId);
    OrderDto calculateTotalCost(String orderId);
    OrderDto calculateDeliveryCost(String orderId);
    OrderDto assembly(String orderId);
    OrderDto assemblyFailed(String orderId);
}