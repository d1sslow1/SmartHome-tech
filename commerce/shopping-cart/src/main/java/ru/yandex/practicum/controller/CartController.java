package ru.yandex.practicum.controller;

import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.dto.CartDto;
import ru.yandex.practicum.dto.CartItemDto;
import ru.yandex.practicum.service.CartService;

import java.util.List;
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
        CartDto cart = cartService.getCart(username);
        for (Map.Entry<String, Integer> entry : products.entrySet()) {
            CartItemDto item = new CartItemDto();
            item.setProductId(entry.getKey());
            item.setQuantity(entry.getValue());
            cart = cartService.addItem(username, item);
        }
        return cart;
    }

    @PostMapping("/change-quantity")
    public CartDto changeQuantity(@RequestParam String username, @RequestBody Map<String, Object> body) {
        // Тесты шлют {"newQuantity": 694, "productId": "uuid"}
        String productId = (String) body.get("productId");
        Integer quantity = (Integer) body.get("newQuantity");

        CartItemDto item = new CartItemDto();
        item.setProductId(productId);
        item.setQuantity(quantity);
        return cartService.updateItem(username, item);
    }

    @PostMapping("/remove")
    public CartDto removeFromCart(@RequestParam String username, @RequestBody List<String> productIds) {
        CartDto cart = cartService.getCart(username);
        for (String productId : productIds) {
            CartItemDto item = new CartItemDto();
            item.setProductId(productId);
            item.setQuantity(0);
            cart = cartService.updateItem(username, item);
        }
        return cart;
    }

    @DeleteMapping
    public void deactivateCart(@RequestParam String username) {
        cartService.deactivateCart(username);
    }
}