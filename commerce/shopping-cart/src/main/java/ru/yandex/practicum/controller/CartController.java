package ru.yandex.practicum.controller;

import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.dto.CartDto;
import ru.yandex.practicum.dto.CartItemDto;
import ru.yandex.practicum.service.CartService;

import java.util.Map;

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
    public CartDto addProductsToCart(@RequestParam String username, @RequestBody Map<String, Integer> products) {
        for (Map.Entry<String, Integer> entry : products.entrySet()) {
            CartItemDto item = new CartItemDto();
            item.setProductId(entry.getKey());
            item.setQuantity(entry.getValue());
            cartService.addItem(username, item);
        }
        return cartService.getCart(username);
    }

    @PostMapping("/change-quantity")
    public CartDto changeQuantity(@RequestParam String username, @RequestBody Map<String, Object> body) {
        String productId = (String) body.get("productId");
        Integer quantity = (Integer) body.get("newQuantity");

        CartItemDto item = new CartItemDto();
        item.setProductId(productId);
        item.setQuantity(quantity);
        cartService.updateItem(username, item);
        return cartService.getCart(username);
    }

    @PostMapping("/remove")
    public CartDto removeFromCart(@RequestParam String username, @RequestBody Map<String, Object> body) {
        String productId = (String) body.get("productId");
        CartItemDto item = new CartItemDto();
        item.setProductId(productId);
        item.setQuantity(0);
        cartService.updateItem(username, item);
        return cartService.getCart(username);
    }

    @DeleteMapping
    public void deactivateCart(@RequestParam String username) {
        cartService.deactivateCart(username);
    }
}