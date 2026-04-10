package ru.yandex.practicum.controller;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.dto.CartDto;
import ru.yandex.practicum.dto.CartItemDto;
import ru.yandex.practicum.service.CartService;

@RestController
public class CartController {

    private final CartService cartService;

    public CartController(CartService cartService) {
        this.cartService = cartService;
    }

    @PostMapping("/api/v1/shopping-cart/{username}/add")
    public ResponseEntity<CartDto> addProductToCart(@PathVariable String username, @RequestBody CartItemDto item) {
        return ResponseEntity.ok(cartService.addItem(username, item));
    }

    @GetMapping("/api/v1/shopping-cart/{username}")
    public ResponseEntity<CartDto> getCart(@PathVariable String username) {
        return ResponseEntity.ok(cartService.getCart(username));
    }

    @PutMapping("/api/v1/shopping-cart/{username}/update")
    public ResponseEntity<CartDto> updateItem(@PathVariable String username, @RequestBody CartItemDto item) {
        return ResponseEntity.ok(cartService.updateItem(username, item));
    }

    @DeleteMapping("/api/v1/shopping-cart/{username}")
    public ResponseEntity<Void> deactivateCart(@PathVariable String username) {
        cartService.deactivateCart(username);
        return ResponseEntity.ok().build();
    }

    @PostMapping("/api/v1/shopping-cart/{username}/remove")
    public ResponseEntity<CartDto> removeItem(@PathVariable String username, @RequestBody CartItemDto item) {
        return ResponseEntity.ok(cartService.removeItem(username, item));
    }

    @PostMapping("/api/v1/shopping-cart/{username}/change-quantity")
    public ResponseEntity<CartDto> changeQuantity(@PathVariable String username, @RequestBody CartItemDto item) {
        return ResponseEntity.ok(cartService.updateItem(username, item));
    }
}