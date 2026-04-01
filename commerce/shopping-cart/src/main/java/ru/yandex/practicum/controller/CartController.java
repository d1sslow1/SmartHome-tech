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
                                 @RequestBody CartItemDto cartItem) {
        log.info("POST /{}/add", username);
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
                                      @RequestBody ChangeProductQuantityRequest request) {
        log.info("PUT /{}/change-quantity", username);
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
}