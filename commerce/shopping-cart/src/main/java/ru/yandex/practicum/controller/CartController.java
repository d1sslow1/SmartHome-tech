package ru.yandex.practicum.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.dto.CartItemDto;
import ru.yandex.practicum.dto.ChangeProductQuantityRequest;
import ru.yandex.practicum.service.CartService;

import java.util.*;

@Slf4j
@RestController
@RequestMapping("/api/v1/shopping-cart")
@RequiredArgsConstructor
public class CartController {

    private final CartService cartService;

    @GetMapping
    public List<Map<String, Object>> getCart(@RequestParam String username) {
        log.info("GET /api/v1/shopping-cart?username={}", username);
        Map<UUID, Integer> cart = cartService.getCart(username);
        List<Map<String, Object>> result = new ArrayList<>();
        for (Map.Entry<UUID, Integer> entry : cart.entrySet()) {
            Map<String, Object> item = new HashMap<>();
            item.put("productId", entry.getKey().toString());
            item.put("quantity", entry.getValue());
            result.add(item);
        }
        return result;
    }

    @PostMapping
    @ResponseStatus(HttpStatus.OK)
    public List<Map<String, Object>> addProductToCart(@RequestBody Map<String, Object> request) {
        log.info("POST /api/v1/shopping-cart");
        String username = (String) request.get("username");
        String productIdStr = (String) request.get("productId");
        Integer quantity = (Integer) request.get("quantity");

        if (username == null || productIdStr == null || quantity == null) {
            throw new IllegalArgumentException("username, productId and quantity are required");
        }

        CartItemDto cartItem = new CartItemDto();
        cartItem.setProductId(UUID.fromString(productIdStr));
        cartItem.setQuantity(quantity);
        cartService.addProductToCart(username, cartItem);

        return getCart(username);
    }

    @DeleteMapping
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void removeProductFromCart(@RequestParam String username, @RequestParam String productId) {
        log.info("DELETE /api/v1/shopping-cart?username={}&productId={}", username, productId);
        cartService.removeProductFromCart(username, UUID.fromString(productId));
    }

    @PutMapping
    @ResponseStatus(HttpStatus.OK)
    public List<Map<String, Object>> updateCart(@RequestParam String username, @RequestBody List<Map<String, Integer>> items) {
        log.info("PUT /api/v1/shopping-cart?username={}", username);

        cartService.clearCart(username);
        for (Map<String, Integer> item : items) {
            for (Map.Entry<String, Integer> entry : item.entrySet()) {
                CartItemDto cartItem = new CartItemDto();
                cartItem.setProductId(UUID.fromString(entry.getKey()));
                cartItem.setQuantity(entry.getValue());
                cartService.addProductToCart(username, cartItem);
            }
        }

        return getCart(username);
    }

    @PostMapping("/change-quantity")
    @ResponseStatus(HttpStatus.OK)
    public List<Map<String, Object>> changeQuantity(@RequestParam String username, @RequestBody Map<String, Object> request) {
        log.info("POST /change-quantity?username={}", username);

        String productIdStr = (String) request.get("productId");
        Integer newQuantity = (Integer) request.get("newQuantity");

        if (productIdStr != null && newQuantity != null) {
            ChangeProductQuantityRequest changeRequest = new ChangeProductQuantityRequest();
            changeRequest.setProductId(UUID.fromString(productIdStr));
            changeRequest.setNewQuantity(newQuantity);
            cartService.changeProductQuantity(username, changeRequest);
        }

        return getCart(username);
    }

    @PostMapping("/remove")
    @ResponseStatus(HttpStatus.OK)
    public List<Map<String, Object>> removeProduct(@RequestParam String username, @RequestBody List<String> productIds) {
        log.info("POST /remove?username={}", username);

        for (String productId : productIds) {
            cartService.removeProductFromCart(username, UUID.fromString(productId));
        }

        return getCart(username);
    }

    @PostMapping("/deactivate")
    @ResponseStatus(HttpStatus.OK)
    public void deactivateCart(@RequestParam String username) {
        log.info("POST /deactivate?username={}", username);
        cartService.deactivateCart(username);
    }
}