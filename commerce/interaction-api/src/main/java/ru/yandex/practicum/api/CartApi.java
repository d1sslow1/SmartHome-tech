package ru.yandex.practicum.api;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.dto.CartDto;
import ru.yandex.practicum.dto.CartItemDto;

public interface CartApi {
    @PostMapping("/api/v1/shopping-cart/add")
    ResponseEntity<CartDto> addProductToCart(@RequestParam("username") String username, @RequestBody CartItemDto item);

    @GetMapping("/api/v1/shopping-cart/{username}")
    ResponseEntity<CartDto> getCart(@PathVariable("username") String username);

    @PutMapping("/api/v1/shopping-cart/update")
    ResponseEntity<CartDto> updateItem(@RequestParam("username") String username, @RequestBody CartItemDto item);

    @PostMapping("/api/v1/shopping-cart/deactivate")
    ResponseEntity<Void> deactivateCart(@RequestParam("username") String username);
}