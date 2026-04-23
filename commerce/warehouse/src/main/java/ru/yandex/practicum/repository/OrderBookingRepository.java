package ru.yandex.practicum.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import ru.yandex.practicum.model.OrderBooking;

import java.util.Optional;

public interface OrderBookingRepository extends JpaRepository<OrderBooking, String> {
    Optional<OrderBooking> findByOrderId(String orderId);
}