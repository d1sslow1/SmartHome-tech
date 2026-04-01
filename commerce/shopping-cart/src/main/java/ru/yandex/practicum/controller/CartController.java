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

    @GetMapping
    public Map<UUID, Integer> getCartByParam(@RequestParam String username) {
        log.info("GET /api/v1/shopping-cart?username={}", username);
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
    @PostMapping("/add")
    @ResponseStatus(HttpStatus.OK)
    public void addProductToCartByBody(@RequestBody Map<String, Object> request) {
        String username = (String) request.get("username");
        String productIdStr = (String) request.get("productId");
        Integer quantity = (Integer) request.get("quantity");

        log.info("POST /add - username={}, productId={}, quantity={}", username, productIdStr, quantity);

        UUID productId = UUID.fromString(productIdStr);

        CartItemDto cartItem = new CartItemDto();
        cartItem.setProductId(productId);
        cartItem.setQuantity(quantity);

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

    @PostMapping("/remove")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void removeProductByBody(@RequestBody Map<String, Object> request) {
        String username = (String) request.get("username");
        String productIdStr = (String) request.get("productId");

        log.info("POST /remove - username={}, productId={}", username, productIdStr);

        UUID productId = UUID.fromString(productIdStr);

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

    @PostMapping("/change-quantity")
    @ResponseStatus(HttpStatus.OK)
    public void changeQuantityByBody(@RequestBody Map<String, Object> request) {
        String username = (String) request.get("username");
        String productIdStr = (String) request.get("productId");
        Integer newQuantity = (Integer) request.get("quantity");

        log.info("POST /change-quantity - username={}, productId={}, newQuantity={}", username, productIdStr, newQuantity);

        UUID productId = UUID.fromString(productIdStr);

        ChangeProductQuantityRequest changeRequest = new ChangeProductQuantityRequest();
        changeRequest.setProductId(productId);
        changeRequest.setNewQuantity(newQuantity);

        cartService.changeProductQuantity(username, changeRequest);
    }

    @Override
    @DeleteMapping("/{username}/clear")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void clearCart(@PathVariable("username") String username) {
        log.info("DELETE /{}/clear", username);
        cartService.clearCart(username);
    }

    @PostMapping("/{username}/clear")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void clearCartPost(@PathVariable("username") String username) {
        log.info("POST /{}/clear", username);
        cartService.clearCart(username);
    }

    @Override
    @PostMapping("/{username}/deactivate")
    @ResponseStatus(HttpStatus.OK)
    public void deactivateCart(@PathVariable("username") String username) {
        log.info("POST /{}/deactivate", username);
        cartService.deactivateCart(username);
    }

    @PostMapping("/deactivate")
    @ResponseStatus(HttpStatus.OK)
    public void deactivateCartByBody(@RequestBody Map<String, Object> request) {
        String username = (String) request.get("username");
        log.info("POST /deactivate - username={}", username);
        cartService.deactivateCart(username);
    }

    @PutMapping
    @ResponseStatus(HttpStatus.OK)
    public void updateCart(@RequestParam String username,
                           @RequestBody Map<UUID, Integer> items) {
        log.info("PUT /api/v1/shopping-cart?username={}", username);
        cartService.clearCart(username);
        for (Map.Entry<UUID, Integer> entry : items.entrySet()) {
            CartItemDto cartItem = new CartItemDto();
            cartItem.setProductId(entry.getKey());
            cartItem.setQuantity(entry.getValue());
            cartService.addProductToCart(username, cartItem);
        }
    }

    @PostMapping
    @ResponseStatus(HttpStatus.OK)
    public void updateCartPost(@RequestParam String username,
                               @RequestBody Map<UUID, Integer> items) {
        log.info("POST /api/v1/shopping-cart?username={}", username);
        cartService.clearCart(username);
        for (Map.Entry<UUID, Integer> entry : items.entrySet()) {
            CartItemDto cartItem = new CartItemDto();
            cartItem.setProductId(entry.getKey());
            cartItem.setQuantity(entry.getValue());
            cartService.addProductToCart(username, cartItem);
        }
    }
}