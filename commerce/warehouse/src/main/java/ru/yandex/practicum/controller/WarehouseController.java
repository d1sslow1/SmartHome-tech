package ru.yandex.practicum.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.client.WarehouseClient;
import ru.yandex.practicum.dto.AddressDto;
import ru.yandex.practicum.dto.CartItemDto;
import ru.yandex.practicum.dto.ShippingDto;
import ru.yandex.practicum.model.WarehouseProduct;
import ru.yandex.practicum.service.WarehouseService;

import java.util.List;
import java.util.Map;
import java.util.UUID;

@Slf4j
@RestController
@RequestMapping("/api/v1/warehouse")
@RequiredArgsConstructor
public class WarehouseController implements WarehouseClient {

    private final WarehouseService warehouseService;

    @Override
    @PostMapping(value = "/check", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public Map<UUID, Boolean> checkAvailability(@RequestBody List<CartItemDto> items) {
        log.info("POST /check - checking availability for {} items", items.size());
        return warehouseService.checkAvailability(items);
    }

    @Override
    @GetMapping(value = "/address", produces = MediaType.APPLICATION_JSON_VALUE)
    public AddressDto getWarehouseAddress() {
        log.info("GET /address - getting warehouse address");
        return warehouseService.getWarehouseAddress();
    }

    @Override
    @PostMapping(value = "/shipping", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public ShippingDto getShippingCost(@RequestBody List<CartItemDto> items) {
        log.info("POST /shipping - calculating shipping cost for {} items", items.size());
        return warehouseService.getShippingCost(items);
    }

    @GetMapping("/{productId}")
    public WarehouseProduct getProduct(@PathVariable UUID productId) {
        log.info("GET /{}", productId);
        return warehouseService.getProduct(productId);
    }

    @GetMapping
    public List<WarehouseProduct> getAllProducts() {
        log.info("GET /");
        return warehouseService.getAllProducts();
    }

    @PostMapping(value = "/products", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.CREATED)
    public WarehouseProduct addProduct(@RequestBody WarehouseProduct product) {
        log.info("POST /products - adding product to warehouse");
        return warehouseService.addProductToWarehouse(product);
    }

    @PutMapping(value = "/products/{productId}/quantity")
    public WarehouseProduct updateQuantity(@PathVariable UUID productId,
                                           @RequestParam Integer quantity) {
        log.info("PUT /products/{}/quantity - updating quantity to {}", productId, quantity);
        return warehouseService.updateProductQuantity(productId, quantity);
    }
}