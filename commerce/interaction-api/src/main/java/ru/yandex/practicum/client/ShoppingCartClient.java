package ru.yandex.practicum.client;

import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.dto.CartItemDto;
import ru.yandex.practicum.dto.ChangeProductQuantityRequest;

import java.util.Map;
import java.util.UUID;

@FeignClient(name = "shopping-cart", path = "/api/v1/shopping-cart")
public interface ShoppingCartClient {

    @GetMapping("/{username}")
    Map<UUID, Integer> getCart(@PathVariable("username") String username);

    @PostMapping("/{username}/add")
    void addProductToCart(@PathVariable("username") String username,
                          @RequestBody CartItemDto cartItem);

    @DeleteMapping("/{username}/remove/{productId}")
    void removeProductFromCart(@PathVariable("username") String username,
                               @PathVariable("productId") UUID productId);

    @PutMapping("/{username}/change-quantity")
    void changeProductQuantity(@PathVariable("username") String username,
                               @RequestBody ChangeProductQuantityRequest request);

    @DeleteMapping("/{username}/clear")
    void clearCart(@PathVariable("username") String username);

    @PostMapping("/{username}/deactivate")
    void deactivateCart(@PathVariable("username") String username);
}