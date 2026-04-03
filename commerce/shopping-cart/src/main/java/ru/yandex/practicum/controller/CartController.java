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

    @GetMapping("/{username}")
    public List<Map<UUID, Integer>> getCart(@PathVariable("username") String username) {
        log.info("GET /{}", username);
        Map<UUID, Integer> result = cartService.getCart(username);
        return List.of(result != null ? result : new HashMap<>());
    }

    @GetMapping
    public List<Map<UUID, Integer>> getCartByParam(@RequestParam String username) {
        log.info("GET /api/v1/shopping-cart?username={}", username);
        Map<UUID, Integer> result = cartService.getCart(username);
        return List.of(result != null ? result : new HashMap<>());
    }

    @PostMapping("/{username}/add")
    @ResponseStatus(HttpStatus.OK)
    public List<Map<UUID, Integer>> addProductToCart(@PathVariable("username") String username,
                                                     @RequestBody CartItemDto cartItem) {
        log.info("POST /{}/add - cartItem: {}", username, cartItem);
        if (cartItem == null || cartItem.getProductId() == null) {
            throw new IllegalArgumentException("ProductId cannot be null");
        }
        cartService.addProductToCart(username, cartItem);
        Map<UUID, Integer> result = cartService.getCart(username);
        return List.of(result != null ? result : new HashMap<>());
    }

    @PostMapping
    @ResponseStatus(HttpStatus.OK)
    public List<Map<UUID, Integer>> addProductToCartDirect(@RequestBody Map<String, Object> request) {
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
        Map<UUID, Integer> result = cartService.getCart(username);
        return List.of(result != null ? result : new HashMap<>());
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
    public void deleteCart(@RequestParam String username) {
        log.info("DELETE /api/v1/shopping-cart?username={}", username);
        cartService.clearCart(username);
    }

    @PostMapping("/remove")
    @ResponseStatus(HttpStatus.OK)
    public List<Map<UUID, Integer>> removeProductPost(@RequestParam String username, @RequestBody Object body) {
        log.info("POST /remove?username={}", username);

        if (body instanceof List) {
            List<?> list = (List<?>) body;
            for (Object item : list) {
                if (item instanceof String) {
                    cartService.removeProductFromCart(username, UUID.fromString((String) item));
                }
            }
        } else if (body instanceof String) {
            cartService.removeProductFromCart(username, UUID.fromString((String) body));
        }

        Map<UUID, Integer> result = cartService.getCart(username);
        return List.of(result != null ? result : new HashMap<>());
    }

    @PutMapping("/{username}/change-quantity")
    @ResponseStatus(HttpStatus.OK)
    public List<Map<UUID, Integer>> changeProductQuantity(@PathVariable("username") String username,
                                                          @RequestBody ChangeProductQuantityRequest request) {
        log.info("PUT /{}/change-quantity - request: {}", username, request);
        if (request == null || request.getProductId() == null) {
            throw new IllegalArgumentException("ProductId cannot be null");
        }
        cartService.changeProductQuantity(username, request);
        Map<UUID, Integer> result = cartService.getCart(username);
        return List.of(result != null ? result : new HashMap<>());
    }

    @PostMapping("/change-quantity")
    @ResponseStatus(HttpStatus.OK)
    public List<Map<UUID, Integer>> changeProductQuantityPost(@RequestParam String username, @RequestBody Map<String, Object> request) {
        log.info("POST /change-quantity?username={}", username);
        String productIdStr = (String) request.get("productId");
        Integer newQuantity = (Integer) request.get("newQuantity");

        if (productIdStr != null && newQuantity != null) {
            ChangeProductQuantityRequest changeRequest = new ChangeProductQuantityRequest();
            changeRequest.setProductId(UUID.fromString(productIdStr));
            changeRequest.setNewQuantity(newQuantity);
            cartService.changeProductQuantity(username, changeRequest);
        }
        Map<UUID, Integer> result = cartService.getCart(username);
        return List.of(result != null ? result : new HashMap<>());
    }

    @PutMapping
    @ResponseStatus(HttpStatus.OK)
    public List<Map<UUID, Integer>> updateCartDirect(@RequestParam String username, @RequestBody Map<UUID, Integer> items) {
        log.info("PUT /api/v1/shopping-cart?username={}", username);
        if (items != null && !items.isEmpty()) {
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
        Map<UUID, Integer> result = cartService.getCart(username);
        return List.of(result != null ? result : new HashMap<>());
    }

    @DeleteMapping("/{username}/clear")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void clearCart(@PathVariable("username") String username) {
        log.info("DELETE /{}/clear", username);
        cartService.clearCart(username);
    }

    @PostMapping("/clear")
    @ResponseStatus(HttpStatus.OK)
    public List<Map<UUID, Integer>> clearCartPost(@RequestParam String username) {
        log.info("POST /clear?username={}", username);
        cartService.clearCart(username);
        Map<UUID, Integer> result = cartService.getCart(username);
        return List.of(result != null ? result : new HashMap<>());
    }

    @PostMapping("/{username}/deactivate")
    @ResponseStatus(HttpStatus.OK)
    public void deactivateCart(@PathVariable("username") String username) {
        log.info("POST /{}/deactivate", username);
        cartService.deactivateCart(username);
    }

    @PostMapping("/deactivate")
    @ResponseStatus(HttpStatus.OK)
    public void deactivateCartDirect(@RequestParam String username) {
        log.info("POST /deactivate?username={}", username);
        cartService.deactivateCart(username);
    }
}