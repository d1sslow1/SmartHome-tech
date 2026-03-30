package ru.yandex.practicum.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ShippingDto {
    private BigDecimal deliveryCost;
    private boolean fragile;
    private BigDecimal weight;
    private BigDecimal volume;
}