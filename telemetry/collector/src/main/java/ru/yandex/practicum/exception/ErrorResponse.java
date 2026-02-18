package ru.yandex.practicum.exception;

import lombok.Builder;
import lombok.Data;
import lombok.experimental.FieldDefaults;

import java.time.LocalDateTime;

@Data
@Builder
@FieldDefaults
public class ErrorResponse {
    LocalDateTime timestamp;
    int status;
    String error;
    String message;
}