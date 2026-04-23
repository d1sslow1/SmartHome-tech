package ru.yandex.practicum.controller;

import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.api.WarehouseApi;
import ru.yandex.practicum.dto.*;
import ru.yandex.practicum.service.WarehouseService;

import java.util.Map;

@RestController
public class WarehouseController implements WarehouseApi {

    private final WarehouseService warehouseService;

    public WarehouseController(WarehouseService warehouseService) {
        this.warehouseService = warehouseService;
    }

    @Override
    @GetMapping("/warehouse/address")
    public AddressDto getWarehouseAddress() {
        return warehouseService.getCurrentAddress();
    }

    @Override
    @PostMapping("/warehouse/check")
    public BookedProductsDto checkProductQuantityEnoughForShoppingCart(@RequestBody ShoppingCartDto cart) {
        return warehouseService.checkAvailability(cart);
    }

    @Override
    @PostMapping("/warehouse/assembly")
    public BookedProductsDto assemblyProductsForOrder(@RequestBody AssemblyProductsForOrderRequest request) {
        return warehouseService.assemblyProducts(request);
    }

    @Override
    @PostMapping("/warehouse/add")
    public void addProductToWarehouse(@RequestBody AddProductToWarehouseRequest request) {
        warehouseService.addItem(request);
    }

    @Override
    @PutMapping("/warehouse")
    public void newProductInWarehouse(@RequestBody NewProductInWarehouseRequest request) {
        warehouseService.newProduct(request);
    }

    @Override
    @PostMapping("/warehouse/shipped")
    public void shippedToDelivery(@RequestBody ShippedToDeliveryRequest request) {
        warehouseService.shippedToDelivery(request);
    }

    @Override
    @PostMapping("/warehouse/return")
    public void acceptReturn(@RequestBody Map<String, Integer> products) {
        warehouseService.acceptReturn(products);
    }
}