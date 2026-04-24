package ru.yandex.practicum.api;

import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.dto.CreateNewOrderRequest;
import ru.yandex.practicum.dto.OrderDto;
import ru.yandex.practicum.dto.ProductReturnRequest;

import java.util.List;

public interface OrderApi {

    String BASE_PATH = "/order";

    @GetMapping(BASE_PATH)
    List<OrderDto> getClientOrders(@RequestParam String username);

    @PutMapping(BASE_PATH)
    OrderDto createNewOrder(@RequestBody CreateNewOrderRequest request);

    @PostMapping(BASE_PATH + "/return")
    OrderDto productReturn(@RequestBody ProductReturnRequest request);

    @PostMapping(BASE_PATH + "/payment")
    OrderDto payment(@RequestBody String orderId);

    @PostMapping(BASE_PATH + "/payment/failed")
    OrderDto paymentFailed(@RequestBody String orderId);

    @PostMapping(BASE_PATH + "/delivery")
    OrderDto delivery(@RequestBody String orderId);

    @PostMapping(BASE_PATH + "/delivery/failed")
    OrderDto deliveryFailed(@RequestBody String orderId);

    @PostMapping(BASE_PATH + "/completed")
    OrderDto complete(@RequestBody String orderId);

    @PostMapping(BASE_PATH + "/calculate/total")
    OrderDto calculateTotalCost(@RequestBody String orderId);

    @PostMapping(BASE_PATH + "/calculate/delivery")
    OrderDto calculateDeliveryCost(@RequestBody String orderId);

    @PostMapping(BASE_PATH + "/assembly")
    OrderDto assembly(@RequestBody String orderId);

    @PostMapping(BASE_PATH + "/assembly/failed")
    OrderDto assemblyFailed(@RequestBody String orderId);
}