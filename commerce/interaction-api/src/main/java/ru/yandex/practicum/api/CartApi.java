package ru.yandex.practicum.api;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.dto.CartDto;
import ru.yandex.practicum.dto.CartItemDto;

public interface CartApi {
    @PostMapping("/api/v1/shopping-cart/{username}/add")
    ResponseEntity<CartDto> addProductToCart(@PathVariable String username, @RequestBody CartItemDto item);

    @GetMapping("/api/v1/shopping-cart/{username}")
    ResponseEntity<CartDto> getCart(@PathVariable String username);

    @PutMapping("/api/v1/shopping-cart/{username}/update")
    ResponseEntity<CartDto> updateItem(@PathVariable String username, @RequestBody CartItemDto item);

    @DeleteMapping("/api/v1/shopping-cart/{username}")
    ResponseEntity<Void> deactivateCart(@PathVariable String username);
}