package ru.yandex.practicum.service;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.address.WarehouseAddress;
import ru.yandex.practicum.dto.*;
import ru.yandex.practicum.model.OrderBooking;
import ru.yandex.practicum.model.WarehouseItem;
import ru.yandex.practicum.repository.OrderBookingRepository;
import ru.yandex.practicum.repository.WarehouseRepository;

import java.util.Map;

@Service
@Transactional
public class WarehouseServiceImpl implements WarehouseService {

    private final WarehouseRepository warehouseRepository;
    private final OrderBookingRepository orderBookingRepository;
    private final WarehouseAddress warehouseAddress;

    public WarehouseServiceImpl(WarehouseRepository warehouseRepository,
                                OrderBookingRepository orderBookingRepository,
                                WarehouseAddress warehouseAddress) {
        this.warehouseRepository = warehouseRepository;
        this.orderBookingRepository = orderBookingRepository;
        this.warehouseAddress = warehouseAddress;
    }

    @Override
    public void addItem(AddProductToWarehouseRequest request) {
        WarehouseItem item = warehouseRepository.findByProductId(request.getProductId())
                .orElseThrow(() -> new RuntimeException("Product not found in warehouse: " + request.getProductId()));
        item.setQuantity(item.getQuantity() + request.getQuantity());
        warehouseRepository.save(item);
    }

    @Override
    public void newProduct(NewProductInWarehouseRequest request) {
        if (warehouseRepository.findByProductId(request.getProductId()).isPresent()) {
            throw new RuntimeException("Product already exists: " + request.getProductId());
        }

        WarehouseItem item = new WarehouseItem();
        item.setProductId(request.getProductId());
        item.setFragile(request.getFragile() != null && request.getFragile());
        item.setWeight(request.getWeight());
        item.setWidth(request.getDimension().getWidth());
        item.setHeight(request.getDimension().getHeight());
        item.setDepth(request.getDimension().getDepth());
        item.setQuantity(0);
        warehouseRepository.save(item);
    }

    @Override
    public BookedProductsDto checkAvailability(ShoppingCartDto cart) {
        BookedProductsDto result = new BookedProductsDto();
        double totalWeight = 0;
        double totalVolume = 0;
        boolean fragile = false;

        for (Map.Entry<String, Integer> entry : cart.getProducts().entrySet()) {
            WarehouseItem item = warehouseRepository.findByProductId(entry.getKey())
                    .orElseThrow(() -> new RuntimeException("Product not found: " + entry.getKey()));

            if (item.getQuantity() < entry.getValue()) {
                throw new RuntimeException("Not enough quantity for product: " + entry.getKey());
            }

            totalWeight += item.getWeight() * entry.getValue();
            totalVolume += item.getWidth() * item.getHeight() * item.getDepth() * entry.getValue();
            if (item.isFragile()) fragile = true;
        }

        result.setDeliveryWeight(totalWeight);
        result.setDeliveryVolume(totalVolume);
        result.setFragile(fragile);
        return result;
    }

    @Override
    public BookedProductsDto assemblyProducts(AssemblyProductsForOrderRequest request) {
        BookedProductsDto result = new BookedProductsDto();
        double totalWeight = 0;
        double totalVolume = 0;
        boolean fragile = false;

        OrderBooking booking = new OrderBooking();
        booking.setOrderId(request.getOrderId());
        booking.setProducts(request.getProducts());

        for (Map.Entry<String, Integer> entry : request.getProducts().entrySet()) {
            WarehouseItem item = warehouseRepository.findByProductId(entry.getKey())
                    .orElseThrow(() -> new RuntimeException("Product not found: " + entry.getKey()));

            if (item.getQuantity() < entry.getValue()) {
                throw new RuntimeException("Not enough quantity for product: " + entry.getKey());
            }

            // Уменьшаем количество на складе
            item.setQuantity(item.getQuantity() - entry.getValue());
            warehouseRepository.save(item);

            totalWeight += item.getWeight() * entry.getValue();
            totalVolume += item.getWidth() * item.getHeight() * item.getDepth() * entry.getValue();
            if (item.isFragile()) fragile = true;
        }

        orderBookingRepository.save(booking);

        result.setDeliveryWeight(totalWeight);
        result.setDeliveryVolume(totalVolume);
        result.setFragile(fragile);
        return result;
    }

    @Override
    public void shippedToDelivery(ShippedToDeliveryRequest request) {
        OrderBooking booking = orderBookingRepository.findByOrderId(request.getOrderId())
                .orElseThrow(() -> new RuntimeException("Booking not found: " + request.getOrderId()));
        booking.setDeliveryId(request.getDeliveryId());
        orderBookingRepository.save(booking);
    }

    @Override
    public void acceptReturn(Map<String, Integer> products) {
        for (Map.Entry<String, Integer> entry : products.entrySet()) {
            WarehouseItem item = warehouseRepository.findByProductId(entry.getKey())
                    .orElseThrow(() -> new RuntimeException("Product not found: " + entry.getKey()));
            item.setQuantity(item.getQuantity() + entry.getValue());
            warehouseRepository.save(item);
        }
    }

    @Override
    public AddressDto getCurrentAddress() {
        return warehouseAddress.getAddress();
    }
}