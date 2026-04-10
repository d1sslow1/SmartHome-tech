package ru.yandex.practicum.service;

import org.springframework.stereotype.Service;
import ru.yandex.practicum.client.WarehouseClient;
import ru.yandex.practicum.dto.CartDto;
import ru.yandex.practicum.dto.CartItemDto;
import ru.yandex.practicum.dto.WarehouseCheckRequestDto;
import ru.yandex.practicum.dto.WarehouseCheckResponseDto;
import ru.yandex.practicum.model.Cart;
import ru.yandex.practicum.model.CartItem;
import ru.yandex.practicum.repository.CartRepository;

import java.util.List;

@Service
public class CartServiceImpl implements CartService {

    private final CartRepository cartRepository;
    private final WarehouseClient warehouseClient;

    public CartServiceImpl(CartRepository cartRepository, WarehouseClient warehouseClient) {
        this.cartRepository = cartRepository;
        this.warehouseClient = warehouseClient;
    }

    @Override
    public CartDto getCart(String username) {
        Cart cart = getOrCreateCart(username);
        return toDto(cart);
    }

    @Override
    public CartDto addItem(String username, CartItemDto itemDto) {
        Cart cart = getOrCreateCart(username);

        if (!cart.isActive()) {
            throw new RuntimeException("Cart is deactivated");
        }

        WarehouseCheckRequestDto request = new WarehouseCheckRequestDto();
        request.setItems(List.of(itemDto));
        WarehouseCheckResponseDto response = warehouseClient.checkAvailability(request);

        if (!response.isAvailable(itemDto.getProductId())) {
            throw new RuntimeException("Product not available in warehouse");
        }

        cart.getItems().stream()
                .filter(i -> i.getProductId().equals(itemDto.getProductId()))
                .findFirst()
                .ifPresentOrElse(
                        item -> item.setQuantity(item.getQuantity() + itemDto.getQuantity()),
                        () -> {
                            CartItem newItem = new CartItem();
                            newItem.setProductId(itemDto.getProductId());
                            newItem.setQuantity(itemDto.getQuantity());
                            cart.getItems().add(newItem);
                        }
                );

        cartRepository.save(cart);
        return toDto(cart);
    }

    @Override
    public CartDto updateItem(String username, CartItemDto itemDto) {
        Cart cart = getOrCreateCart(username);

        if (!cart.isActive()) {
            throw new RuntimeException("Cart is deactivated");
        }

        cart.getItems().stream()
                .filter(i -> i.getProductId().equals(itemDto.getProductId()))
                .findFirst()
                .ifPresent(item -> item.setQuantity(itemDto.getQuantity()));

        cartRepository.save(cart);
        return toDto(cart);
    }

    @Override
    public CartDto removeItem(String username, CartItemDto itemDto) {
        Cart cart = getOrCreateCart(username);

        if (!cart.isActive()) {
            throw new RuntimeException("Cart is deactivated");
        }

        cart.getItems().removeIf(i -> i.getProductId().equals(itemDto.getProductId()));
        cartRepository.save(cart);
        return toDto(cart);
    }

    @Override
    public void deactivateCart(String username) {
        Cart cart = getOrCreateCart(username);
        cart.setActive(false);
        cartRepository.save(cart);
    }

    private Cart getOrCreateCart(String username) {
        return cartRepository.findByUsernameAndActiveTrue(username)
                .orElseGet(() -> {
                    Cart cart = new Cart();
                    cart.setUsername(username);
                    cart.setActive(true);
                    return cartRepository.save(cart);
                });
    }

    private CartDto toDto(Cart cart) {
        CartDto dto = new CartDto();
        dto.setUsername(cart.getUsername());
        dto.setActive(cart.isActive());
        dto.setItems(cart.getItems().stream()
                .map(item -> {
                    CartItemDto i = new CartItemDto();
                    i.setProductId(item.getProductId());
                    i.setQuantity(item.getQuantity());
                    return i;
                }).toList());
        return dto;
    }
}