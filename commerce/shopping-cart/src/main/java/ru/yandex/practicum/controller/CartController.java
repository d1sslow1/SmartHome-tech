package ru.yandex.practicum.controller;

import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.dto.CartDto;
import ru.yandex.practicum.dto.CartItemDto;
import ru.yandex.practicum.service.CartService;

@RestController
@RequestMapping("/shopping-cart")
public class CartController {

    private final CartService cartService;

    public CartController(CartService cartService) {
        this.cartService = cartService;
    }

    @GetMapping
    public CartDto getCart(@RequestParam String username) {
        return cartService.getCart(username);
    }

    @PutMapping
    public CartDto addProductToCart(@RequestParam String username, @RequestBody CartItemDto item) {
        return cartService.addItem(username, item);
    }

    @PostMapping("/change-quantity")
    public CartDto changeQuantity(@RequestParam String username, @RequestBody CartItemDto item) {
        return cartService.updateItem(username, item);
    }

    @PostMapping("/remove")
    public CartDto removeFromCart(@RequestParam String username, @RequestBody CartItemDto item) {
        // Устанавливаем количество 0 для удаления
        item.setQuantity(0);
        return cartService.updateItem(username, item);
    }

    @DeleteMapping
    public void deactivateCart(@RequestParam String username) {
        cartService.deactivateCart(username);
    }
}