package ru.yandex.practicum;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;

@SpringBootApplication
@ConfigurationPropertiesScan
@EnableDiscoveryClient
public class CollectorApplication {

    public static void main(String[] args) {
        System.out.println("=== COLLECTOR STARTING ===");
        System.out.println("Java version: " + System.getProperty("java.version"));
        System.out.println("Working directory: " + System.getProperty("user.dir"));

        try {
            SpringApplication.run(CollectorApplication.class, args);
            System.out.println("=== COLLECTOR STARTED SUCCESSFULLY ===");
        } catch (Exception e) {
            System.out.println("=== COLLECTOR FAILED TO START ===");
            e.printStackTrace();
            throw e;
        }
    }
}