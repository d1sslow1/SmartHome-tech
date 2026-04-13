package ru.yandex.practicum.dto;

import java.util.HashMap;
import java.util.Map;

public class WarehouseCheckResponseDto {
    private Map<String, Boolean> availability;

    public WarehouseCheckResponseDto() {
        this.availability = new HashMap<>();
    }

    public Map<String, Boolean> getAvailability() { return availability; }
    public void setAvailability(Map<String, Boolean> availability) { this.availability = availability; }
    public boolean isAvailable(String productId) {
        return availability.getOrDefault(productId, false);
    }
    public void addAvailability(String productId, boolean available) {
        availability.put(productId, available);
    }
}