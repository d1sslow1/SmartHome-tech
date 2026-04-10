package ru.yandex.practicum.controller;

import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.dto.WarehouseAddressDto;
import ru.yandex.practicum.dto.WarehouseCheckRequestDto;
import ru.yandex.practicum.dto.WarehouseCheckResponseDto;
import ru.yandex.practicum.dto.WarehouseItemDto;
import ru.yandex.practicum.service.WarehouseService;

@RestController
public class WarehouseController {

    private final WarehouseService warehouseService;

    public WarehouseController(WarehouseService warehouseService) {
        this.warehouseService = warehouseService;
    }

    @GetMapping("/api/v1/warehouse/address")
    public WarehouseAddressDto getCurrentAddress() {
        return warehouseService.getCurrentAddress();
    }

    @PostMapping("/api/v1/warehouse/check")
    public WarehouseCheckResponseDto checkAvailability(@RequestBody WarehouseCheckRequestDto request) {
        return warehouseService.checkAvailability(request);
    }

    @PostMapping("/api/v1/warehouse/add")
    public void addItem(@RequestBody WarehouseItemDto dto) {
        warehouseService.addItem(dto);
    }

    @PutMapping("/api/v1/warehouse")
    public void addOrUpdateItem(@RequestBody WarehouseItemDto dto) {
        warehouseService.addItem(dto);
    }
}