package ru.yandex.practicum.controller;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.dto.CartDto;
import ru.yandex.practicum.dto.CartItemDto;
import ru.yandex.practicum.service.CartService;

import java.util.List;
import java.util.Map;

@RestController
public class CartController {

    private final CartService cartService;

    public CartController(CartService cartService) {
        this.cartService = cartService;
    }

    @PostMapping("/api/v1/shopping-cart/add")
    public ResponseEntity<CartDto> addProductToCart(@RequestParam String username, @RequestBody CartItemDto item) {
        return ResponseEntity.ok(cartService.addItem(username, item));
    }

    @GetMapping("/api/v1/shopping-cart")
    public ResponseEntity<CartDto> getCart(@RequestParam String username) {
        return ResponseEntity.ok(cartService.getCart(username));
    }

    @PutMapping("/api/v1/shopping-cart")
    public ResponseEntity<CartDto> updateCart(@RequestParam String username, @RequestBody Map<String, Integer> items) {
        for (Map.Entry<String, Integer> entry : items.entrySet()) {
            CartItemDto item = new CartItemDto();
            item.setProductId(entry.getKey());
            item.setQuantity(entry.getValue());
            cartService.updateItem(username, item);
        }
        return ResponseEntity.ok(cartService.getCart(username));
    }

    @PostMapping("/api/v1/shopping-cart/update")
    public ResponseEntity<CartDto> updateItem(@RequestParam String username, @RequestBody CartItemDto item) {
        return ResponseEntity.ok(cartService.updateItem(username, item));
    }

    @DeleteMapping("/api/v1/shopping-cart")
    public ResponseEntity<Void> deactivateCart(@RequestParam String username) {
        cartService.deactivateCart(username);
        return ResponseEntity.ok().build();
    }

    @PostMapping("/api/v1/shopping-cart/remove")
    public ResponseEntity<CartDto> removeItem(@RequestParam String username, @RequestBody List<String> productIds) {
        for (String productId : productIds) {
            CartItemDto item = new CartItemDto();
            item.setProductId(productId);
            cartService.removeItem(username, item);
        }
        return ResponseEntity.ok(cartService.getCart(username));
    }

    @PostMapping("/api/v1/shopping-cart/change-quantity")
    public ResponseEntity<CartDto> changeQuantity(@RequestParam String username, @RequestBody Map<String, Integer> request) {
        for (Map.Entry<String, Integer> entry : request.entrySet()) {
            CartItemDto item = new CartItemDto();
            item.setProductId(entry.getKey());
            item.setQuantity(entry.getValue());
            cartService.updateItem(username, item);
        }
        return ResponseEntity.ok(cartService.getCart(username));
    }
}