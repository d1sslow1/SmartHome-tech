package ru.yandex.practicum.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.dto.AddressDto;
import ru.yandex.practicum.dto.CartItemDto;
import ru.yandex.practicum.dto.ShippingDto;
import ru.yandex.practicum.exception.NotFoundException;
import ru.yandex.practicum.model.WarehouseProduct;
import ru.yandex.practicum.repository.WarehouseProductRepository;
import ru.yandex.practicum.config.WarehouseAddressConfig;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class WarehouseService {

    private final WarehouseProductRepository warehouseProductRepository;
    private final WarehouseAddressConfig addressConfig;

    public Map<UUID, Boolean> checkAvailability(List<CartItemDto> items) {
        log.info("Checking availability for {} items", items.size());

        Map<UUID, Boolean> result = new HashMap<>();

        for (CartItemDto item : items) {
            UUID productId = item.getProductId();
            Integer requestedQuantity = item.getQuantity();

            WarehouseProduct warehouseProduct = warehouseProductRepository.findById(productId)
                    .orElse(null);

            boolean available = warehouseProduct != null &&
                    warehouseProduct.getQuantity() >= requestedQuantity;

            result.put(productId, available);
            log.debug("Product {} availability: {} (requested: {}, available: {})",
                    productId, available, requestedQuantity,
                    warehouseProduct != null ? warehouseProduct.getQuantity() : 0);
        }

        return result;
    }

    public AddressDto getWarehouseAddress() {
        log.debug("Getting warehouse address: {}", addressConfig.getCurrentAddress());
        return addressConfig.getCurrentAddress();
    }

    public ShippingDto getShippingCost(List<CartItemDto> items) {
        log.info("Calculating shipping cost for {} items", items.size());

        double totalWeight = 0;
        double totalVolume = 0;
        boolean hasFragile = false;

        for (CartItemDto item : items) {
            WarehouseProduct product = warehouseProductRepository.findById(item.getProductId())
                    .orElseThrow(() -> new NotFoundException("Product not found: " + item.getProductId()));

            double itemVolume = product.getWidth() * product.getHeight() * product.getDepth();
            totalWeight += product.getWeight() * item.getQuantity();
            totalVolume += itemVolume * item.getQuantity();

            if (product.getFragile()) {
                hasFragile = true;
            }
        }

        BigDecimal deliveryCost = BigDecimal.valueOf(totalWeight * 0.5 + totalVolume * 0.1);
        if (hasFragile) {
            deliveryCost = deliveryCost.add(BigDecimal.valueOf(100));
        }

        ShippingDto shippingDto = ShippingDto.builder()
                .deliveryCost(deliveryCost)
                .fragile(hasFragile)
                .weight(BigDecimal.valueOf(totalWeight))
                .volume(BigDecimal.valueOf(totalVolume))
                .build();

        log.info("Calculated shipping cost: {}", shippingDto);
        return shippingDto;
    }

    public WarehouseProduct addProductToWarehouse(WarehouseProduct product) {
        log.info("Adding product to warehouse: {}", product.getProductId());
        return warehouseProductRepository.save(product);
    }

    public WarehouseProduct updateProductQuantity(UUID productId, Integer newQuantity) {
        WarehouseProduct product = warehouseProductRepository.findById(productId)
                .orElseThrow(() -> new NotFoundException("Product not found: " + productId));

        product.setQuantity(newQuantity);
        log.info("Updated product {} quantity to {}", productId, newQuantity);
        return warehouseProductRepository.save(product);
    }
}