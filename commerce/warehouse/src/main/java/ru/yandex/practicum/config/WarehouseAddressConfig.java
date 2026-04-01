package ru.yandex.practicum.config;

import jakarta.annotation.PostConstruct;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.dto.AddressDto;

import java.security.SecureRandom;
import java.util.List;
import java.util.Random;

@Slf4j
@Component
@Getter
public class WarehouseAddressConfig {

    @Value("${warehouse.addresses:ADDRESS_1,ADDRESS_2}")
    private List<String> addresses;

    private AddressDto currentAddress;

    @PostConstruct
    public void init() {
        Random random = new SecureRandom();
        String selectedAddress = addresses.get(random.nextInt(addresses.size()));

        log.info("Initializing warehouse with address: {}", selectedAddress);

        currentAddress = AddressDto.builder()
                .country(selectedAddress)
                .city(selectedAddress)
                .street(selectedAddress)
                .house(selectedAddress)
                .flat(selectedAddress)
                .build();

        log.info("Warehouse address initialized: {}", currentAddress);
    }
}