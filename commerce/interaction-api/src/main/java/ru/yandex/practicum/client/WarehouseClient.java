package ru.yandex.practicum.client;

import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.dto.AddressDto;
import ru.yandex.practicum.dto.CartItemDto;
import ru.yandex.practicum.dto.ShippingDto;

import java.util.List;
import java.util.Map;
import java.util.UUID;

@FeignClient(name = "warehouse", path = "/api/v1/warehouse")
public interface WarehouseClient {

    @PostMapping("/check")
    Map<UUID, Boolean> checkAvailability(@RequestBody List<CartItemDto> items);

    @GetMapping("/address")
    AddressDto getWarehouseAddress();

    @PostMapping("/shipping")
    ShippingDto getShippingCost(@RequestBody List<CartItemDto> items);
}