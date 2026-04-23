package ru.yandex.practicum.service;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.address.WarehouseAddressProvider;
import ru.yandex.practicum.client.OrderClient;
import ru.yandex.practicum.client.WarehouseClient;
import ru.yandex.practicum.dto.*;
import ru.yandex.practicum.model.Delivery;
import ru.yandex.practicum.repository.DeliveryRepository;

@Service
@Transactional
public class DeliveryServiceImpl implements DeliveryService {

    private final DeliveryRepository deliveryRepository;
    private final WarehouseAddressProvider addressProvider;
    private final WarehouseClient warehouseClient;
    private final OrderClient orderClient;

    public DeliveryServiceImpl(DeliveryRepository deliveryRepository,
                               WarehouseAddressProvider addressProvider,
                               WarehouseClient warehouseClient,
                               OrderClient orderClient) {
        this.deliveryRepository = deliveryRepository;
        this.addressProvider = addressProvider;
        this.warehouseClient = warehouseClient;
        this.orderClient = orderClient;
    }

    @Override
    public DeliveryDto planDelivery(DeliveryDto dto) {
        Delivery delivery = new Delivery();
        delivery.setOrderId(dto.getOrderId());
        delivery.setDeliveryState("CREATED");

        AddressDto from = dto.getFromAddress();
        if (from != null) {
            delivery.setFromCountry(from.getCountry());
            delivery.setFromCity(from.getCity());
            delivery.setFromStreet(from.getStreet());
            delivery.setFromHouse(from.getHouse());
            delivery.setFromFlat(from.getFlat());
        }

        AddressDto to = dto.getToAddress();
        if (to != null) {
            delivery.setToCountry(to.getCountry());
            delivery.setToCity(to.getCity());
            delivery.setToStreet(to.getStreet());
            delivery.setToHouse(to.getHouse());
            delivery.setToFlat(to.getFlat());
        }

        delivery = deliveryRepository.save(delivery);

        DeliveryDto result = new DeliveryDto();
        result.setDeliveryId(delivery.getDeliveryId());
        result.setOrderId(delivery.getOrderId());
        result.setDeliveryState(delivery.getDeliveryState());

        AddressDto fromDto = new AddressDto(delivery.getFromCountry(), delivery.getFromCity(),
                delivery.getFromStreet(), delivery.getFromHouse(), delivery.getFromFlat());
        result.setFromAddress(fromDto);

        AddressDto toDto = new AddressDto(delivery.getToCountry(), delivery.getToCity(),
                delivery.getToStreet(), delivery.getToHouse(), delivery.getToFlat());
        result.setToAddress(toDto);

        return result;
    }

    @Override
    public void deliverySuccessful(String orderId) {
        Delivery delivery = deliveryRepository.findByOrderId(orderId)
                .orElseThrow(() -> new RuntimeException("Delivery not found"));
        delivery.setDeliveryState("DELIVERED");
        deliveryRepository.save(delivery);

        orderClient.delivery(orderId);
    }

    @Override
    public void deliveryPicked(String orderId) {
        Delivery delivery = deliveryRepository.findByOrderId(orderId)
                .orElseThrow(() -> new RuntimeException("Delivery not found"));
        delivery.setDeliveryState("IN_PROGRESS");
        deliveryRepository.save(delivery);

        ShippedToDeliveryRequest shippedRequest = new ShippedToDeliveryRequest();
        shippedRequest.setOrderId(orderId);
        shippedRequest.setDeliveryId(delivery.getDeliveryId());
        warehouseClient.shippedToDelivery(shippedRequest);

        orderClient.assembly(orderId);
    }

    @Override
    public void deliveryFailed(String orderId) {
        Delivery delivery = deliveryRepository.findByOrderId(orderId)
                .orElseThrow(() -> new RuntimeException("Delivery not found"));
        delivery.setDeliveryState("FAILED");
        deliveryRepository.save(delivery);

        orderClient.deliveryFailed(orderId);
    }

    @Override
    public Double deliveryCost(OrderDto order) {
        AddressDto warehouseAddress = addressProvider.getAddress();
        String warehouseName = warehouseAddress.getStreet();
        double cost = 5.0;

        if ("ADDRESS_1".equals(warehouseName)) {
            cost += cost * 1.0;
        } else if ("ADDRESS_2".equals(warehouseName)) {
            cost += cost * 2.0;
        }

        if (order.getFragile() != null && order.getFragile()) {
            cost += cost * 0.2;
        }

        if (order.getDeliveryWeight() != null) {
            cost += order.getDeliveryWeight() * 0.3;
        }

        if (order.getDeliveryVolume() != null) {
            cost += order.getDeliveryVolume() * 0.2;
        }

        if (order.getDeliveryAddress() != null && !warehouseName.equals(order.getDeliveryAddress().getStreet())) {
            cost += cost * 0.2;
        }

        return cost;
    }
}