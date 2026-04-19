package ru.yandex.practicum.api;

import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.dto.ProductDto;
import ru.yandex.practicum.enums.ProductAvailability;
import ru.yandex.practicum.enums.ProductCategory;

import java.util.List;

public interface StoreApi {

    String BASE_PATH = "/api/v1/shopping-store";

    @GetMapping(BASE_PATH + "/{id}")
    ProductDto getProduct(@PathVariable Long id);

    @GetMapping(BASE_PATH)
    List<ProductDto> getProducts(@RequestParam(required = false) ProductCategory category);

    @PostMapping(BASE_PATH)
    ProductDto addProduct(@RequestBody ProductDto product);

    @PutMapping(BASE_PATH + "/{id}")
    ProductDto updateProduct(@PathVariable Long id, @RequestBody ProductDto product);

    @DeleteMapping(BASE_PATH + "/{id}")
    void deactivateProduct(@PathVariable Long id);

    @PutMapping(BASE_PATH + "/{id}/quantityState")
    void updateQuantityState(@PathVariable Long id, @RequestParam ProductAvailability quantityState);
}