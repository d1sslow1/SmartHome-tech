package ru.yandex.practicum.api;

import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.dto.WarehouseAddressDto;
import ru.yandex.practicum.dto.WarehouseCheckRequestDto;
import ru.yandex.practicum.dto.WarehouseCheckResponseDto;
import ru.yandex.practicum.dto.WarehouseItemDto;

public interface WarehouseApi {
    @GetMapping("/api/v1/warehouse/address")
    WarehouseAddressDto getCurrentAddress();

    @PostMapping("/api/v1/warehouse/check")
    WarehouseCheckResponseDto checkAvailability(@RequestBody WarehouseCheckRequestDto request);

    @PostMapping("/api/v1/warehouse/add")
    void addItem(@RequestBody WarehouseItemDto dto);

    @PutMapping("/api/v1/warehouse/update")
    void updateQuantity(@RequestParam Long productId, @RequestParam int quantity);
}