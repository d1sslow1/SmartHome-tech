package ru.yandex.practicum.controller;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.api.CartApi;
import ru.yandex.practicum.dto.CartDto;
import ru.yandex.practicum.dto.CartItemDto;
import ru.yandex.practicum.service.CartService;

@RestController
@RequestMapping("/api/v1/shopping-cart")
public class CartController implements CartApi {

    private final CartService cartService;

    public CartController(CartService cartService) {
        this.cartService = cartService;
    }

    @Override
    @PostMapping("/add")
    public ResponseEntity<CartDto> addProductToCart(@RequestParam String username, @RequestBody CartItemDto item) {
        return ResponseEntity.ok(cartService.addItem(username, item));
    }

    @Override
    @GetMapping("/{username}")
    public ResponseEntity<CartDto> getCart(@PathVariable String username) {
        return ResponseEntity.ok(cartService.getCart(username));
    }

    @Override
    @PutMapping("/update")
    public ResponseEntity<CartDto> updateItem(@RequestParam String username, @RequestBody CartItemDto item) {
        return ResponseEntity.ok(cartService.updateItem(username, item));
    }

    @Override
    @PostMapping("/deactivate")
    public ResponseEntity<Void> deactivateCart(@RequestParam String username) {
        cartService.deactivateCart(username);
        return ResponseEntity.ok().build();
    }
}