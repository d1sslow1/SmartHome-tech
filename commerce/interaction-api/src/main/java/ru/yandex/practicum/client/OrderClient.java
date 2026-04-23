package ru.yandex.practicum.client;

import org.springframework.cloud.openfeign.FeignClient;
import ru.yandex.practicum.api.OrderApi;

@FeignClient(name = "order", path = "/api/v1")
public interface OrderClient extends OrderApi {
}