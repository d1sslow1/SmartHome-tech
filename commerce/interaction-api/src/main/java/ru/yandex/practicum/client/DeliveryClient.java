package ru.yandex.practicum.client;

import org.springframework.cloud.openfeign.FeignClient;
import ru.yandex.practicum.api.DeliveryApi;

@FeignClient(name = "delivery", path = "/api/v1")
public interface DeliveryClient extends DeliveryApi {
}