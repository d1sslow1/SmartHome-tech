package ru.yandex.practicum.service;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.client.*;
import ru.yandex.practicum.dto.*;
import ru.yandex.practicum.model.Order;
import ru.yandex.practicum.repository.OrderRepository;

import java.util.*;

@Service
@Transactional
public class OrderServiceImpl implements OrderService {

    private final OrderRepository orderRepository;
    private final WarehouseClient warehouseClient;
    private final DeliveryClient deliveryClient;
    private final PaymentClient paymentClient;

    public OrderServiceImpl(OrderRepository orderRepository,
                            WarehouseClient warehouseClient,
                            DeliveryClient deliveryClient,
                            PaymentClient paymentClient) {
        this.orderRepository = orderRepository;
        this.warehouseClient = warehouseClient;
        this.deliveryClient = deliveryClient;
        this.paymentClient = paymentClient;
    }

    @Override
    public List<OrderDto> getClientOrders(String username) {
        List<Order> orders = orderRepository.findByShoppingCartId(username + "_cart");
        return orders.stream().map(this::toDto).toList();
    }

    @Override
    public OrderDto createNewOrder(CreateNewOrderRequest request) {
        ShoppingCartDto cart = request.getShoppingCart();
        AddressDto deliveryAddress = request.getDeliveryAddress();

        // Проверяем наличие товаров на складе
        BookedProductsDto booked = warehouseClient.checkProductQuantityEnoughForShoppingCart(cart);

        Order order = new Order();
        order.setShoppingCartId(cart.getShoppingCartId());
        order.setProducts(cart.getProducts());
        order.setState("NEW");
        order.setDeliveryWeight(booked.getDeliveryWeight());
        order.setDeliveryVolume(booked.getDeliveryVolume());
        order.setFragile(booked.getFragile());

        // Рассчитываем доставку
        OrderDto tempDto = toDto(order);
        tempDto.setDeliveryAddress(deliveryAddress);
        Double deliveryCost = deliveryClient.deliveryCost(tempDto);
        order.setDeliveryPrice(deliveryCost);

        // Рассчитываем стоимость товаров
        tempDto = toDto(order);
        tempDto.setDeliveryAddress(deliveryAddress);
        Double productCost = paymentClient.productCost(tempDto);
        order.setProductPrice(productCost);

        // Рассчитываем общую стоимость
        tempDto = toDto(order);
        tempDto.setDeliveryAddress(deliveryAddress);
        Double totalCost = paymentClient.getTotalCost(tempDto);
        order.setTotalPrice(totalCost);

        order = orderRepository.save(order);

        // Создаём доставку
        DeliveryDto delivery = new DeliveryDto();
        delivery.setOrderId(order.getOrderId());
        delivery.setDeliveryState("CREATED");

        // Получаем адрес склада
        AddressDto warehouseAddress = warehouseClient.getWarehouseAddress();
        delivery.setFromAddress(warehouseAddress);
        delivery.setToAddress(deliveryAddress);

        DeliveryDto savedDelivery = deliveryClient.planDelivery(delivery);
        order.setDeliveryId(savedDelivery.getDeliveryId());
        orderRepository.save(order);

        return toDto(order);
    }

    @Override
    public OrderDto productReturn(ProductReturnRequest request) {
        Order order = orderRepository.findByOrderId(request.getOrderId())
                .orElseThrow(() -> new RuntimeException("Order not found"));

        order.setState("PRODUCT_RETURNED");
        orderRepository.save(order);

        // Возвращаем товары на склад
        warehouseClient.acceptReturn(request.getProducts());

        return toDto(order);
    }

    @Override
    public OrderDto payment(String orderId) {
        Order order = orderRepository.findByOrderId(orderId)
                .orElseThrow(() -> new RuntimeException("Order not found"));
        order.setState("PAID");
        orderRepository.save(order);
        return toDto(order);
    }

    @Override
    public OrderDto paymentFailed(String orderId) {
        Order order = orderRepository.findByOrderId(orderId)
                .orElseThrow(() -> new RuntimeException("Order not found"));
        order.setState("PAYMENT_FAILED");
        orderRepository.save(order);
        return toDto(order);
    }

    @Override
    public OrderDto delivery(String orderId) {
        Order order = orderRepository.findByOrderId(orderId)
                .orElseThrow(() -> new RuntimeException("Order not found"));
        order.setState("DELIVERED");
        orderRepository.save(order);
        return toDto(order);
    }

    @Override
    public OrderDto deliveryFailed(String orderId) {
        Order order = orderRepository.findByOrderId(orderId)
                .orElseThrow(() -> new RuntimeException("Order not found"));
        order.setState("DELIVERY_FAILED");
        orderRepository.save(order);
        return toDto(order);
    }

    @Override
    public OrderDto complete(String orderId) {
        Order order = orderRepository.findByOrderId(orderId)
                .orElseThrow(() -> new RuntimeException("Order not found"));
        order.setState("COMPLETED");
        orderRepository.save(order);
        return toDto(order);
    }

    @Override
    public OrderDto calculateTotalCost(String orderId) {
        Order order = orderRepository.findByOrderId(orderId)
                .orElseThrow(() -> new RuntimeException("Order not found"));
        OrderDto dto = toDto(order);
        Double totalCost = paymentClient.getTotalCost(dto);
        order.setTotalPrice(totalCost);
        orderRepository.save(order);
        return toDto(order);
    }

    @Override
    public OrderDto calculateDeliveryCost(String orderId) {
        Order order = orderRepository.findByOrderId(orderId)
                .orElseThrow(() -> new RuntimeException("Order not found"));
        OrderDto dto = toDto(order);
        Double deliveryCost = deliveryClient.deliveryCost(dto);
        order.setDeliveryPrice(deliveryCost);
        orderRepository.save(order);
        return toDto(order);
    }

    @Override
    public OrderDto assembly(String orderId) {
        Order order = orderRepository.findByOrderId(orderId)
                .orElseThrow(() -> new RuntimeException("Order not found"));

        AssemblyProductsForOrderRequest assemblyRequest = new AssemblyProductsForOrderRequest();
        assemblyRequest.setOrderId(orderId);
        assemblyRequest.setProducts(order.getProducts());

        warehouseClient.assemblyProductsForOrder(assemblyRequest);
        order.setState("ASSEMBLED");
        orderRepository.save(order);
        return toDto(order);
    }

    @Override
    public OrderDto assemblyFailed(String orderId) {
        Order order = orderRepository.findByOrderId(orderId)
                .orElseThrow(() -> new RuntimeException("Order not found"));
        order.setState("ASSEMBLY_FAILED");
        orderRepository.save(order);
        return toDto(order);
    }

    private OrderDto toDto(Order order) {
        OrderDto dto = new OrderDto();
        dto.setOrderId(order.getOrderId());
        dto.setShoppingCartId(order.getShoppingCartId());
        dto.setProducts(order.getProducts());
        dto.setPaymentId(order.getPaymentId());
        dto.setDeliveryId(order.getDeliveryId());
        dto.setState(order.getState());
        dto.setDeliveryWeight(order.getDeliveryWeight());
        dto.setDeliveryVolume(order.getDeliveryVolume());
        dto.setFragile(order.getFragile());
        dto.setTotalPrice(order.getTotalPrice());
        dto.setDeliveryPrice(order.getDeliveryPrice());
        dto.setProductPrice(order.getProductPrice());
        return dto;
    }
}