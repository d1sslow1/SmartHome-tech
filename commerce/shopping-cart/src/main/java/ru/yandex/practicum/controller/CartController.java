package ru.yandex.practicum.controller;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.api.CartApi;
import ru.yandex.practicum.dto.CartDto;
import ru.yandex.practicum.dto.CartItemDto;
import ru.yandex.practicum.service.CartService;

@RestController
public class CartController implements CartApi {

    private final CartService cartService;

    public CartController(CartService cartService) {
        this.cartService = cartService;
    }

    @Override
    public ResponseEntity<CartDto> addProductToCart(String username, CartItemDto item) {
        return ResponseEntity.ok(cartService.addItem(username, item));
    }

    @Override
    public ResponseEntity<CartDto> getCart(String username) {
        return ResponseEntity.ok(cartService.getCart(username));
    }

    @Override
    public ResponseEntity<CartDto> updateItem(String username, CartItemDto item) {
        return ResponseEntity.ok(cartService.updateItem(username, item));
    }

    @Override
    public ResponseEntity<Void> deactivateCart(String username) {
        cartService.deactivateCart(username);
        return ResponseEntity.ok().build();
    }
}