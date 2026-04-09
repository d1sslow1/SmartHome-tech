package ru.yandex.practicum.controller;

import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.api.WarehouseApi;
import ru.yandex.practicum.dto.WarehouseAddressDto;
import ru.yandex.practicum.dto.WarehouseCheckRequestDto;
import ru.yandex.practicum.dto.WarehouseCheckResponseDto;
import ru.yandex.practicum.dto.WarehouseItemDto;
import ru.yandex.practicum.service.WarehouseService;

@RestController
@RequestMapping("/api/v1/warehouse")
public class WarehouseController implements WarehouseApi {

    private final WarehouseService warehouseService;

    public WarehouseController(WarehouseService warehouseService) {
        this.warehouseService = warehouseService;
    }

    @Override
    @GetMapping("/address")
    public WarehouseAddressDto getCurrentAddress() {
        return warehouseService.getCurrentAddress();
    }

    @Override
    @PostMapping("/check")
    public WarehouseCheckResponseDto checkAvailability(@RequestBody WarehouseCheckRequestDto request) {
        return warehouseService.checkAvailability(request);
    }

    @Override
    @PostMapping("/add")
    public void addItem(@RequestBody WarehouseItemDto dto) {
        warehouseService.addItem(dto);
    }

    @Override
    @PutMapping("/update")
    public void updateQuantity(@RequestParam Long productId, @RequestParam int quantity) {
        warehouseService.updateQuantity(productId, quantity);
    }
}