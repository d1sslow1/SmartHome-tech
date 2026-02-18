package ru.yandex.practicum.exception;

import lombok.Builder;
import lombok.Data;
import lombok.experimental.FieldDefaults;
import lombok.AccessLevel;
import java.time.LocalDateTime;

@Data
@Builder
@FieldDefaults(level = AccessLevel.PRIVATE)
public class ErrorResponse {
    LocalDateTime timestamp;
    int status;
    String error;
    String message;
}