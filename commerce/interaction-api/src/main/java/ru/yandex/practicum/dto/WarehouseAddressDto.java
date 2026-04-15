package ru.yandex.practicum.dto;

public class WarehouseAddressDto {
    private String country;
    private String city;
    private String street;
    private String building;
    private String house;

    public WarehouseAddressDto() {}

    public WarehouseAddressDto(String country, String city, String street, String building, String house) {
        this.country = country;
        this.city = city;
        this.street = street;
        this.building = building;
        this.house = house;
    }

    public String getCountry() { return country; }
    public void setCountry(String country) { this.country = country; }
    public String getCity() { return city; }
    public void setCity(String city) { this.city = city; }
    public String getStreet() { return street; }
    public void setStreet(String street) { this.street = street; }
    public String getBuilding() { return building; }
    public void setBuilding(String building) { this.building = building; }
    public String getHouse() { return house; }
    public void setHouse(String house) { this.house = house; }
}