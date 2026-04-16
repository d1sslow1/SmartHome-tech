package ru.yandex.practicum.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

public class WarehouseAddressDto {
    private String country;
    private String city;
    private String street;

    @JsonProperty("house")  // Тесты ждут house!
    private String building;

    private String apartment;

    public WarehouseAddressDto() {}

    public WarehouseAddressDto(String country, String city, String street, String building, String apartment) {
        this.country = country;
        this.city = city;
        this.street = street;
        this.building = building;
        this.apartment = apartment;
    }

    public String getCountry() { return country; }
    public void setCountry(String country) { this.country = country; }

    public String getCity() { return city; }
    public void setCity(String city) { this.city = city; }

    public String getStreet() { return street; }
    public void setStreet(String street) { this.street = street; }

    public String getBuilding() { return building; }
    public void setBuilding(String building) { this.building = building; }

    public String getApartment() { return apartment; }
    public void setApartment(String apartment) { this.apartment = apartment; }
}