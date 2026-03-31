package ru.yandex.practicum.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.UUID;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class CartResponseDto {
    private UUID cartId;
    private String username;
    private String state;
    private java.util.Map<UUID, Integer> items;
}