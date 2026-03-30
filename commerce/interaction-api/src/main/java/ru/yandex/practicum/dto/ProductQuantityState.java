package ru.yandex.practicum.dto;

public enum ProductQuantityState {
    ENDED,      // товар закончился
    FEW,        // осталось меньше 10 единиц
    ENOUGH,     // осталось от 10 до 100 единиц
    MANY        // осталось больше 100 единиц
}