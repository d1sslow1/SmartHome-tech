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
                                 @RequestBody CartItemDto cartItem) {
        log.info("POST /{}/add - cartItem: {}", username, cartItem);
        if (cartItem == null || cartItem.getProductId() == null) {
            throw new IllegalArgumentException("ProductId cannot be null");
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
                                      @RequestBody ChangeProductQuantityRequest request) {
        log.info("PUT /{}/change-quantity - request: {}", username, request);
        if (request == null || request.getProductId() == null) {
            throw new IllegalArgumentException("ProductId cannot be null");
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

    @PostMapping
    @ResponseStatus(HttpStatus.OK)
    public void addProductToCartDirect(@RequestBody Map<String, Object> request) {
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
    }

    @PutMapping
    @ResponseStatus(HttpStatus.OK)
    public void changeQuantityDirect(@RequestBody Map<String, Object> request) {
        log.info("PUT /api/v1/shopping-cart");
        String username = (String) request.get("username");
        String productIdStr = (String) request.get("productId");
        Integer newQuantity = (Integer) request.get("quantity");

        if (username == null || productIdStr == null || newQuantity == null) {
            throw new IllegalArgumentException("username, productId and quantity are required");
        }

        ChangeProductQuantityRequest changeRequest = new ChangeProductQuantityRequest();
        changeRequest.setProductId(UUID.fromString(productIdStr));
        changeRequest.setNewQuantity(newQuantity);
        cartService.changeProductQuantity(username, changeRequest);
    }

    @DeleteMapping
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void removeProductDirect(@RequestParam String username, @RequestParam UUID productId) {
        log.info("DELETE /api/v1/shopping-cart?username={}&productId={}", username, productId);
        cartService.removeProductFromCart(username, productId);
    }
}