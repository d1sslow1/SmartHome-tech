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
import java.util.Optional;

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
            throw new RuntimeException("Корзина деактивирована");
        }

        // Проверяем наличие на складе
        try {
            WarehouseCheckRequestDto checkRequest = new WarehouseCheckRequestDto();
            checkRequest.setItems(List.of(itemDto));
            WarehouseCheckResponseDto availability = warehouseClient.checkAvailability(checkRequest);

            if (!availability.isAvailable(itemDto.getProductId())) {
                throw new RuntimeException("Товар недоступен в нужном количестве");
            }
        } catch (Exception e) {
            throw new RuntimeException("Ошибка проверки склада: " + e.getMessage());
        }

        Optional<CartItem> existingItem = cart.getItems().stream()
                .filter(item -> item.getProductId().equals(itemDto.getProductId()))
                .findFirst();

        if (existingItem.isPresent()) {
            CartItem cartItem = existingItem.get();
            cartItem.setQuantity(cartItem.getQuantity() + itemDto.getQuantity());
        } else {
            CartItem newCartItem = new CartItem();
            newCartItem.setProductId(itemDto.getProductId());
            newCartItem.setQuantity(itemDto.getQuantity());
            cart.getItems().add(newCartItem);
        }

        cartRepository.save(cart);
        return toDto(cart);
    }

    @Override
    public CartDto updateItem(String username, CartItemDto itemDto) {
        Cart cart = getOrCreateCart(username);

        if (!cart.isActive()) {
            throw new RuntimeException("Корзина деактивирована");
        }

        if (itemDto.getQuantity() <= 0) {
            cart.getItems().removeIf(cartItem -> cartItem.getProductId().equals(itemDto.getProductId()));
        } else {
            boolean itemFound = false;
            for (CartItem cartItem : cart.getItems()) {
                if (cartItem.getProductId().equals(itemDto.getProductId())) {
                    cartItem.setQuantity(itemDto.getQuantity());
                    itemFound = true;
                    break;
                }
            }
            if (!itemFound) {
                CartItem newCartItem = new CartItem();
                newCartItem.setProductId(itemDto.getProductId());
                newCartItem.setQuantity(itemDto.getQuantity());
                cart.getItems().add(newCartItem);
            }
        }

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
        return cartRepository.findByUsernameAndActiveTrue(username).orElseGet(() -> {
            Cart newCart = new Cart();
            newCart.setUsername(username);
            return cartRepository.save(newCart);
        });
    }

    private CartDto toDto(Cart cart) {
        CartDto cartDto = new CartDto();
        cartDto.setUsername(cart.getUsername());
        cartDto.setActive(cart.isActive());

        List<CartItemDto> itemDtos = cart.getItems().stream()
                .map(this::toItemDto)
                .toList();
        cartDto.setItems(itemDtos);

        return cartDto;
    }

    private CartItemDto toItemDto(CartItem cartItem) {
        CartItemDto itemDto = new CartItemDto();
        itemDto.setProductId(cartItem.getProductId());
        itemDto.setQuantity(cartItem.getQuantity());
        return itemDto;
    }
}