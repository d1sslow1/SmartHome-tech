package ru.yandex.practicum.api;

import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.dto.CreateNewOrderRequest;
import ru.yandex.practicum.dto.OrderDto;
import ru.yandex.practicum.dto.ProductReturnRequest;

import java.util.List;

public interface OrderApi {

    @GetMapping("/order")
    List<OrderDto> getClientOrders(@RequestParam String username);

    @PutMapping("/order")
    OrderDto createNewOrder(@RequestBody CreateNewOrderRequest request);

    @PostMapping("/order/return")
    OrderDto productReturn(@RequestBody ProductReturnRequest request);

    @PostMapping("/order/payment")
    OrderDto payment(@RequestBody String orderId);

    @PostMapping("/order/payment/failed")
    OrderDto paymentFailed(@RequestBody String orderId);

    @PostMapping("/order/delivery")
    OrderDto delivery(@RequestBody String orderId);

    @PostMapping("/order/delivery/failed")
    OrderDto deliveryFailed(@RequestBody String orderId);

    @PostMapping("/order/completed")
    OrderDto complete(@RequestBody String orderId);

    @PostMapping("/order/calculate/total")
    OrderDto calculateTotalCost(@RequestBody String orderId);

    @PostMapping("/order/calculate/delivery")
    OrderDto calculateDeliveryCost(@RequestBody String orderId);

    @PostMapping("/order/assembly")
    OrderDto assembly(@RequestBody String orderId);

    @PostMapping("/order/assembly/failed")
    OrderDto assemblyFailed(@RequestBody String orderId);
}