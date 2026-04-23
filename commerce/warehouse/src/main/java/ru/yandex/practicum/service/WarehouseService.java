package ru.yandex.practicum.service;

import ru.yandex.practicum.dto.*;

import java.util.Map;

public interface WarehouseService {

    void addItem(AddProductToWarehouseRequest request);

    void newProduct(NewProductInWarehouseRequest request);

    BookedProductsDto checkAvailability(ShoppingCartDto cart);

    BookedProductsDto assemblyProducts(AssemblyProductsForOrderRequest request);

    void shippedToDelivery(ShippedToDeliveryRequest request);

    void acceptReturn(Map<String, Integer> products);

    AddressDto getCurrentAddress();
}