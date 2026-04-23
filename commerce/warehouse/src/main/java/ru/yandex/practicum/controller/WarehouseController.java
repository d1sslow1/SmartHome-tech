package ru.yandex.practicum.controller;

import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.dto.WarehouseAddressDto;
import ru.yandex.practicum.dto.WarehouseCheckRequestDto;
import ru.yandex.practicum.dto.WarehouseCheckResponseDto;
import ru.yandex.practicum.dto.WarehouseItemDto;
import ru.yandex.practicum.service.WarehouseService;

@RestController
@RequestMapping("/warehouse")
public class WarehouseController {

    private final WarehouseService warehouseService;

    public WarehouseController(WarehouseService warehouseService) {
        this.warehouseService = warehouseService;
    }

    @GetMapping("/address")
    public WarehouseAddressDto getAddress() {
        return warehouseService.getCurrentAddress();
    }

    @PutMapping
    public void addNewProduct(@RequestBody WarehouseItemDto dto) {
        warehouseService.addItem(dto);
    }

    @PostMapping("/add")
    public void addProduct(@RequestBody WarehouseItemDto dto) {
        warehouseService.addItem(dto);
    }

    @PostMapping("/check")
    public WarehouseCheckResponseDto checkAvailability(@RequestBody WarehouseCheckRequestDto request) {
        return warehouseService.checkAvailability(request);
    }
}