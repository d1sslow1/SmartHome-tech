package ru.yandex.practicum.exception;

public class CartDeactivatedException extends RuntimeException {

    public CartDeactivatedException(String message) {
        super(message);
    }
}