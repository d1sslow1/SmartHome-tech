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
    public CartDto changeQuantity(@RequestParam String username,
                                  @RequestParam(required = false) String productId,
                                  @RequestParam(required = false) Integer quantity,
                                  @RequestBody(required = false) CartItemDto item) {
        if (item == null && productId != null && quantity != null) {
            item = new CartItemDto();
            item.setProductId(productId);
            item.setQuantity(quantity);
        }
        if (item == null) {
            throw new RuntimeException("Missing product data");
        }
        return cartService.updateItem(username, item);
    }

    @PostMapping("/remove")
    public CartDto removeFromCart(@RequestParam String username,
                                  @RequestParam(required = false) String productId,
                                  @RequestBody(required = false) CartItemDto item) {
        if (item == null && productId != null) {
            item = new CartItemDto();
            item.setProductId(productId);
            item.setQuantity(0);
        }
        if (item == null) {
            throw new RuntimeException("Missing product data");
        }
        item.setQuantity(0);
        return cartService.updateItem(username, item);
    }

    @DeleteMapping
    public void deactivateCart(@RequestParam String username) {
        cartService.deactivateCart(username);
    }
}