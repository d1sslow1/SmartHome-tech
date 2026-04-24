package ru.yandex.practicum.api;

import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.dto.*;

import java.util.Map;

public interface WarehouseApi {

    String BASE_PATH = "/warehouse";

    @GetMapping(BASE_PATH + "/address")
    AddressDto getWarehouseAddress();

    @PostMapping(BASE_PATH + "/check")
    BookedProductsDto checkProductQuantityEnoughForShoppingCart(@RequestBody ShoppingCartDto shoppingCart);

    @PostMapping(BASE_PATH + "/assembly")
    BookedProductsDto assemblyProductsForOrder(@RequestBody AssemblyProductsForOrderRequest request);

    @PostMapping(BASE_PATH + "/add")
    void addProductToWarehouse(@RequestBody AddProductToWarehouseRequest request);

    @PutMapping(BASE_PATH)
    void newProductInWarehouse(@RequestBody NewProductInWarehouseRequest request);

    @PostMapping(BASE_PATH + "/shipped")
    void shippedToDelivery(@RequestBody ShippedToDeliveryRequest request);

    @PostMapping(BASE_PATH + "/return")
    void acceptReturn(@RequestBody Map<String, Integer> products);
}