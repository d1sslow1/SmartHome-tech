package ru.yandex.practicum.exception;

public class ProductNotAvailableException extends RuntimeException {

    public ProductNotAvailableException(String message) {
        super(message);
    }
}