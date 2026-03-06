package ru.yandex.practicum;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class CollectorApplication {
    public static void main(String[] args) {
        System.out.println("=== COLLECTOR STARTING ===");
        System.out.println("gRPC port: 59091");
        SpringApplication.run(CollectorApplication.class, args);
        System.out.println("=== COLLECTOR STARTED ===");
    }
}