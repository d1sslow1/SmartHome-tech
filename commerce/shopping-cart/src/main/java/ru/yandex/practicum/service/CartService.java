package ru.yandex.practicum.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.client.WarehouseClient;
import ru.yandex.practicum.dto.CartItemDto;
import ru.yandex.practicum.dto.ChangeProductQuantityRequest;
import ru.yandex.practicum.exception.CartDeactivatedException;
import ru.yandex.practicum.exception.NotFoundException;
import ru.yandex.practicum.exception.ProductNotAvailableException;
import ru.yandex.practicum.model.Cart;
import ru.yandex.practicum.model.CartItem;
import ru.yandex.practicum.model.CartState;
import ru.yandex.practicum.repository.CartItemRepository;
import ru.yandex.practicum.repository.CartRepository;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
@Transactional(readOnly = true)
public class CartService {

    private final CartRepository cartRepository;
    private final CartItemRepository cartItemRepository;
    private final WarehouseClient warehouseClient;

    public Map<UUID, Integer> getCart(String username) {
        log.info("Getting cart for user: {}", username);

        Cart cart = cartRepository.findByUsername(username)
                .orElseGet(() -> createNewCart(username));

        Map<UUID, Integer> result = new HashMap<>();
        for (CartItem item : cart.getItems()) {
            result.put(item.getProductId(), item.getQuantity());
        }

        return result;
    }

    @Transactional
    public Cart createNewCart(String username) {
        log.info("Creating new cart for user: {}", username);

        Cart cart = new Cart();
        cart.setUsername(username);
        cart.setState(CartState.ACTIVE);

        return cartRepository.save(cart);
    }

    @Transactional
    public void addProductToCart(String username, CartItemDto cartItem) {
        log.info("Adding product {} to cart for user {}", cartItem.getProductId(), username);

        Cart cart = getActiveCart(username);

        try {
            List<CartItemDto> itemsToCheck = List.of(cartItem);
            Map<UUID, Boolean> availability = warehouseClient.checkAvailability(itemsToCheck);
            Boolean isAvailable = availability.get(cartItem.getProductId());
            if (isAvailable == null || !isAvailable) {
                log.warn("Product {} is not available in warehouse", cartItem.getProductId());
                throw new ProductNotAvailableException("Product not available: " + cartItem.getProductId());
            }
        } catch (Exception e) {
            log.error("Error checking availability with warehouse: {}", e.getMessage());
        }

        CartItem existingItem = cart.getItems().stream()
                .filter(item -> item.getProductId().equals(cartItem.getProductId()))
                .findFirst()
                .orElse(null);

        if (existingItem != null) {
            existingItem.setQuantity(existingItem.getQuantity() + cartItem.getQuantity());
            log.debug("Updated quantity for product {} to {}",
                    cartItem.getProductId(), existingItem.getQuantity());
        } else {
            CartItem newItem = new CartItem();
            newItem.setCart(cart);
            newItem.setProductId(cartItem.getProductId());
            newItem.setQuantity(cartItem.getQuantity());
            cart.getItems().add(newItem);
            cartItemRepository.save(newItem);
            log.debug("Added new product {} to cart", cartItem.getProductId());
        }

        cartRepository.save(cart);
    }

    @Transactional
    public void removeProductFromCart(String username, UUID productId) {
        log.info("Removing product {} from cart for user {}", productId, username);

        Cart cart = getActiveCart(username);

        CartItem itemToRemove = cart.getItems().stream()
                .filter(item -> item.getProductId().equals(productId))
                .findFirst()
                .orElseThrow(() -> new NotFoundException("Product not found in cart: " + productId));

        cart.getItems().remove(itemToRemove);
        cartItemRepository.delete(itemToRemove);

        cartRepository.save(cart);
        log.debug("Removed product {} from cart", productId);
    }

    @Transactional
    public void changeProductQuantity(String username, ChangeProductQuantityRequest request) {
        log.info("Changing quantity for product {} to {} for user {}",
                request.getProductId(), request.getNewQuantity(), username);

        Cart cart = getActiveCart(username);

        if (request.getNewQuantity() <= 0) {
            removeProductFromCart(username, request.getProductId());
            return;
        }

        CartItem item = cart.getItems().stream()
                .filter(i -> i.getProductId().equals(request.getProductId()))
                .findFirst()
                .orElseThrow(() -> new NotFoundException("Product not found in cart: " + request.getProductId()));

        item.setQuantity(request.getNewQuantity());
        cartRepository.save(cart);
        log.debug("Updated quantity for product {} to {}", request.getProductId(), request.getNewQuantity());
    }

    @Transactional
    public void clearCart(String username) {
        log.info("Clearing cart for user {}", username);

        Cart cart = getActiveCart(username);

        cartItemRepository.deleteAll(cart.getItems());
        cart.getItems().clear();

        cartRepository.save(cart);
        log.debug("Cleared cart for user {}", username);
    }

    @Transactional
    public void deactivateCart(String username) {
        log.info("Deactivating cart for user {}", username);

        Cart cart = cartRepository.findByUsername(username)
                .orElseThrow(() -> new NotFoundException("Cart not found for user: " + username));

        cart.setState(CartState.DEACTIVATE);
        cartRepository.save(cart);
        log.info("Deactivated cart for user {}", username);
    }

    private Cart getActiveCart(String username) {
        Cart cart = cartRepository.findByUsername(username)
                .orElseGet(() -> createNewCart(username));

        if (cart.getState() == CartState.DEACTIVATE) {
            log.warn("Attempt to modify deactivated cart for user {}", username);
            throw new CartDeactivatedException("Cart is deactivated for user: " + username);
        }

        return cart;
    }
}