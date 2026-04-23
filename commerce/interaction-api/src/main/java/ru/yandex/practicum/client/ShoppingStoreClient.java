package ru.yandex.practicum.client;

import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import ru.yandex.practicum.dto.ProductDto;

@FeignClient(name = "shopping-store", path = "/api/v1")
public interface ShoppingStoreClient {
    @GetMapping("/shopping-store/{id}")
    ProductDto getProduct(@PathVariable Long id);
}