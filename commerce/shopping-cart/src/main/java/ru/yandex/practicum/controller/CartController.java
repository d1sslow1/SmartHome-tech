package ru.yandex.practicum.controller;

import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.dto.CartDto;
import ru.yandex.practicum.dto.CartItemDto;
import ru.yandex.practicum.service.CartService;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/shopping-cart")
public class CartController {

    private final CartService cartService;

    public CartController(CartService cartService) {
        this.cartService = cartService;
    }

    @GetMapping
    public Map<String, Object> getCart(@RequestParam String username) {
        CartDto cart = cartService.getCart(username);
        return formatCartResponse(cart);
    }

    @PutMapping
    public Map<String, Object> addProductsToCart(@RequestParam String username, @RequestBody Map<String, Integer> products) {
        for (Map.Entry<String, Integer> entry : products.entrySet()) {
            CartItemDto item = new CartItemDto();
            item.setProductId(entry.getKey());
            item.setQuantity(entry.getValue());
            cartService.addItem(username, item);
        }
        CartDto cart = cartService.getCart(username);
        return formatCartResponse(cart);
    }

    @PostMapping("/change-quantity")
    public Map<String, Object> changeQuantity(@RequestParam String username, @RequestBody Map<String, Object> body) {
        String productId = (String) body.get("productId");
        Integer quantity = (Integer) body.get("newQuantity");

        CartItemDto item = new CartItemDto();
        item.setProductId(productId);
        item.setQuantity(quantity);
        cartService.updateItem(username, item);
        CartDto cart = cartService.getCart(username);
        return formatCartResponse(cart);
    }

    @PostMapping("/remove")
    public Map<String, Object> removeFromCart(@RequestParam String username, @RequestBody Object body) {
        if (body instanceof List) {
            List<String> productIds = (List<String>) body;
            for (String productId : productIds) {
                CartItemDto item = new CartItemDto();
                item.setProductId(productId);
                item.setQuantity(0);
                cartService.updateItem(username, item);
            }
        } else if (body instanceof Map) {
            Map<String, Object> map = (Map<String, Object>) body;
            String productId = (String) map.get("productId");
            CartItemDto item = new CartItemDto();
            item.setProductId(productId);
            item.setQuantity(0);
            cartService.updateItem(username, item);
        }
        CartDto cart = cartService.getCart(username);
        return formatCartResponse(cart);
    }

    @DeleteMapping
    public void deactivateCart(@RequestParam String username) {
        cartService.deactivateCart(username);
    }

    private Map<String, Object> formatCartResponse(CartDto cart) {
        Map<String, Object> response = new HashMap<>();
        Map<String, Integer> products = cart.getItems().stream()
                .collect(Collectors.toMap(
                        CartItemDto::getProductId,
                        CartItemDto::getQuantity
                ));
        response.put("products", products);
        response.put("shoppingCartId", cart.getUsername() + "_cart");
        return response;
    }
}