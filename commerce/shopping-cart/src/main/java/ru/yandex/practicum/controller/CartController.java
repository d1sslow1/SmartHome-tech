package ru.yandex.practicum.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.dto.CartItemDto;
import ru.yandex.practicum.dto.ChangeProductQuantityRequest;
import ru.yandex.practicum.service.CartService;

import java.util.Map;
import java.util.UUID;

@Slf4j
@RestController
@RequestMapping("/api/v1/shopping-cart")
@RequiredArgsConstructor
public class CartController {

    private final CartService cartService;

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

    @DeleteMapping("/{username}/remove/{productId}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void removeProductFromCart(@PathVariable("username") String username,
                                      @PathVariable("productId") UUID productId) {
        log.info("DELETE /{}/remove/{}", username, productId);
        cartService.removeProductFromCart(username, productId);
    }

    @DeleteMapping
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void removeProductDirect(@RequestParam String username, @RequestParam UUID productId) {
        log.info("DELETE /api/v1/shopping-cart?username={}&productId={}", username, productId);
        cartService.removeProductFromCart(username, productId);
    }

    @PutMapping("/{username}/change-quantity")
    @ResponseStatus(HttpStatus.OK)
    public Map<UUID, Integer> changeProductQuantity(@PathVariable("username") String username,
                                                    @RequestBody ChangeProductQuantityRequest request) {
        log.info("PUT /{}/change-quantity - request: {}", username, request);
        if (request == null || request.getProductId() == null) {
            throw new IllegalArgumentException("ProductId cannot be null");
        }
        cartService.changeProductQuantity(username, request);
        return cartService.getCart(username);
    }

    @PutMapping
    @ResponseStatus(HttpStatus.OK)
    public Map<UUID, Integer> updateCartDirect(@RequestParam String username, @RequestBody Map<UUID, Integer> items) {
        log.info("PUT /api/v1/shopping-cart?username={}", username);
        if (items != null) {
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
        return cartService.getCart(username);
    }

    @DeleteMapping("/{username}/clear")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void clearCart(@PathVariable("username") String username) {
        log.info("DELETE /{}/clear", username);
        cartService.clearCart(username);
    }

    @PostMapping("/{username}/deactivate")
    @ResponseStatus(HttpStatus.OK)
    public void deactivateCart(@PathVariable("username") String username) {
        log.info("POST /{}/deactivate", username);
        cartService.deactivateCart(username);
    }
}