package ru.yandex.practicum.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.client.ShoppingCartClient;
import ru.yandex.practicum.dto.CartItemDto;
import ru.yandex.practicum.dto.ChangeProductQuantityRequest;
import ru.yandex.practicum.service.CartService;

import java.util.Map;
import java.util.UUID;

@Slf4j
@RestController
@RequestMapping("/api/v1/shopping-cart")
@RequiredArgsConstructor
public class CartController implements ShoppingCartClient {

    private final CartService cartService;

    @Override
    @GetMapping("/{username}")
    public Map<UUID, Integer> getCart(@PathVariable("username") String username) {
        log.info("GET /{}", username);
        return cartService.getCart(username);
    }

    @Override
    @PostMapping("/{username}/add")
    @ResponseStatus(HttpStatus.OK)
    public void addProductToCart(@PathVariable("username") String username,
                                 @RequestBody(required = false) CartItemDto cartItem) {
        log.info("POST /{}/add - cartItem: {}", username, cartItem);
        if (cartItem == null || cartItem.getProductId() == null) {
            log.warn("CartItem is null or missing productId, ignoring");
            return;
        }
        cartService.addProductToCart(username, cartItem);
    }

    @Override
    @DeleteMapping("/{username}/remove/{productId}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void removeProductFromCart(@PathVariable("username") String username,
                                      @PathVariable("productId") UUID productId) {
        log.info("DELETE /{}/remove/{}", username, productId);
        cartService.removeProductFromCart(username, productId);
    }

    @Override
    @PutMapping("/{username}/change-quantity")
    @ResponseStatus(HttpStatus.OK)
    public void changeProductQuantity(@PathVariable("username") String username,
                                      @RequestBody(required = false) ChangeProductQuantityRequest request) {
        log.info("PUT /{}/change-quantity - request: {}", username, request);
        if (request == null || request.getProductId() == null) {
            log.warn("Request is null or missing productId, ignoring");
            return;
        }
        cartService.changeProductQuantity(username, request);
    }

    @Override
    @DeleteMapping("/{username}/clear")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void clearCart(@PathVariable("username") String username) {
        log.info("DELETE /{}/clear", username);
        cartService.clearCart(username);
    }

    @Override
    @PostMapping("/{username}/deactivate")
    @ResponseStatus(HttpStatus.OK)
    public void deactivateCart(@PathVariable("username") String username) {
        log.info("POST /{}/deactivate", username);
        cartService.deactivateCart(username);
    }

    @PutMapping("/{username}")
    @ResponseStatus(HttpStatus.OK)
    public void updateCart(@PathVariable("username") String username,
                           @RequestBody(required = false) Map<UUID, Integer> items) {
        log.info("PUT /{} - items: {}", username, items);
        if (items == null) {
            log.warn("Items is null, ignoring");
            return;
        }
        cartService.clearCart(username);
        for (Map.Entry<UUID, Integer> entry : items.entrySet()) {
            if (entry.getValue() != null && entry.getValue() > 0) {
                CartItemDto cartItem = new CartItemDto();
                cartItem.setProductId(entry.getKey());
                cartItem.setQuantity(entry.getValue());
                cartService.addProductToCart(username, cartItem);
            }
        }
    }
}