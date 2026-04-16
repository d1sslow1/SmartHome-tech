package ru.yandex.practicum.client;

import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import ru.yandex.practicum.dto.WarehouseCheckRequestDto;
import ru.yandex.practicum.dto.WarehouseCheckResponseDto;

@FeignClient(name = "warehouse", path = "/api/v1/warehouse")  // Добавлен /api/v1
public interface WarehouseClient {

    @PostMapping("/check")
    WarehouseCheckResponseDto checkAvailability(@RequestBody WarehouseCheckRequestDto request);
}