package ru.yandex.practicum.client;

import org.springframework.cloud.openfeign.FeignClient;
import ru.yandex.practicum.api.PaymentApi;

@FeignClient(name = "payment", path = "/api/v1")
public interface PaymentClient extends PaymentApi {
}