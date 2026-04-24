package ru.yandex.practicum.model;

import jakarta.persistence.*;
import lombok.AccessLevel;
import lombok.experimental.FieldDefaults;

@Entity
@FieldDefaults(level = AccessLevel.PRIVATE)
public class Delivery {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    String deliveryId;

    String fromCountry;
    String fromCity;
    String fromStreet;
    String fromHouse;
    String fromFlat;

    String toCountry;
    String toCity;
    String toStreet;
    String toHouse;
    String toFlat;

    String orderId;
    String deliveryState;
    Double deliveryWeight;
    Double deliveryVolume;
    Boolean fragile;

    public String getDeliveryId() { return deliveryId; }
    public void setDeliveryId(String deliveryId) { this.deliveryId = deliveryId; }

    public String getFromCountry() { return fromCountry; }
    public void setFromCountry(String fromCountry) { this.fromCountry = fromCountry; }

    public String getFromCity() { return fromCity; }
    public void setFromCity(String fromCity) { this.fromCity = fromCity; }

    public String getFromStreet() { return fromStreet; }
    public void setFromStreet(String fromStreet) { this.fromStreet = fromStreet; }

    public String getFromHouse() { return fromHouse; }
    public void setFromHouse(String fromHouse) { this.fromHouse = fromHouse; }

    public String getFromFlat() { return fromFlat; }
    public void setFromFlat(String fromFlat) { this.fromFlat = fromFlat; }

    public String getToCountry() { return toCountry; }
    public void setToCountry(String toCountry) { this.toCountry = toCountry; }

    public String getToCity() { return toCity; }
    public void setToCity(String toCity) { this.toCity = toCity; }

    public String getToStreet() { return toStreet; }
    public void setToStreet(String toStreet) { this.toStreet = toStreet; }

    public String getToHouse() { return toHouse; }
    public void setToHouse(String toHouse) { this.toHouse = toHouse; }

    public String getToFlat() { return toFlat; }
    public void setToFlat(String toFlat) { this.toFlat = toFlat; }

    public String getOrderId() { return orderId; }
    public void setOrderId(String orderId) { this.orderId = orderId; }

    public String getDeliveryState() { return deliveryState; }
    public void setDeliveryState(String deliveryState) { this.deliveryState = deliveryState; }

    public Double getDeliveryWeight() { return deliveryWeight; }
    public void setDeliveryWeight(Double deliveryWeight) { this.deliveryWeight = deliveryWeight; }

    public Double getDeliveryVolume() { return deliveryVolume; }
    public void setDeliveryVolume(Double deliveryVolume) { this.deliveryVolume = deliveryVolume; }

    public Boolean getFragile() { return fragile; }
    public void setFragile(Boolean fragile) { this.fragile = fragile; }
}