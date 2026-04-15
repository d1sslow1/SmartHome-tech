package ru.yandex.practicum.dto;

public class WarehouseAddressDto {
    private String country;
    private String city;
    private String street;
    private String building;
    private String flat;

    public WarehouseAddressDto() {}

    public WarehouseAddressDto(String country, String city, String street, String building, String flat) {
        this.country = country;
        this.city = city;
        this.street = street;
        this.building = building;
        this.flat = flat;
    }

    public String getCountry() { return country; }
    public void setCountry(String country) { this.country = country; }
    public String getCity() { return city; }
    public void setCity(String city) { this.city = city; }
    public String getStreet() { return street; }
    public void setStreet(String street) { this.street = street; }
    public String getBuilding() { return building; }
    public void setBuilding(String building) { this.building = building; }
    public String getFlat() { return flat; }
    public void setFlat(String flat) { this.flat = flat; }
}