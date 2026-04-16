package ru.yandex.practicum.dto;

import java.util.Map;
import java.util.HashMap;

public class WarehouseCheckResponseDto {

    private Map<String, Boolean> availability;  // Было Map<Long, Boolean>

    public WarehouseCheckResponseDto() {
        this.availability = new HashMap<>();
    }

    public WarehouseCheckResponseDto(Map<String, Boolean> availability) {
        this.availability = availability;
    }

    public Map<String, Boolean> getAvailability() {
        return availability;
    }

    public void setAvailability(Map<String, Boolean> availability) {
        this.availability = availability;
    }

    public boolean isAvailable(String productId) {
        return availability.getOrDefault(productId, false);
    }

    public void addAvailability(String productId, boolean available) {
        availability.put(productId, available);
    }
}