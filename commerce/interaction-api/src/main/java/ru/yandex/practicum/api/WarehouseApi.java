package ru.yandex.practicum.api;

import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.dto.*;

import java.util.Map;

public interface WarehouseApi {

    @GetMapping("/warehouse/address")
    AddressDto getWarehouseAddress();

    @PostMapping("/warehouse/check")
    BookedProductsDto checkProductQuantityEnoughForShoppingCart(@RequestBody ShoppingCartDto shoppingCart);

    @PostMapping("/warehouse/assembly")
    BookedProductsDto assemblyProductsForOrder(@RequestBody AssemblyProductsForOrderRequest request);

    @PostMapping("/warehouse/add")
    void addProductToWarehouse(@RequestBody AddProductToWarehouseRequest request);

    @PutMapping("/warehouse")
    void newProductInWarehouse(@RequestBody NewProductInWarehouseRequest request);

    @PostMapping("/warehouse/shipped")
    void shippedToDelivery(@RequestBody ShippedToDeliveryRequest request);

    @PostMapping("/warehouse/return")
    void acceptReturn(@RequestBody Map<String, Integer> products);
}