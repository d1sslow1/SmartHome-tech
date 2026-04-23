package ru.yandex.practicum.address;

import org.springframework.stereotype.Component;
import ru.yandex.practicum.dto.AddressDto;

import java.security.SecureRandom;

@Component
public class WarehouseAddress {

    private static final String[] ADDRESSES = {"ADDRESS_1", "ADDRESS_2"};
    private final String currentAddress;

    public WarehouseAddress() {
        SecureRandom random = new SecureRandom();
        this.currentAddress = ADDRESSES[random.nextInt(ADDRESSES.length)];
    }

    public AddressDto getAddress() {
        AddressDto dto = new AddressDto();
        dto.setCountry(currentAddress);
        dto.setCity(currentAddress);
        dto.setStreet(currentAddress);
        dto.setHouse(currentAddress);
        dto.setFlat(currentAddress);
        return dto;
    }
}