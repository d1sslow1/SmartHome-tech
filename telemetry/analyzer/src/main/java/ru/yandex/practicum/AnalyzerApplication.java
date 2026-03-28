package ru.yandex.practicum;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;

@SpringBootApplication
@ConfigurationPropertiesScan
@EnableDiscoveryClient
public class AnalyzerApplication {

    public static void main(String[] args) {
        System.out.println("=== AnalyzerApplication main() ===");
        System.out.println("Current directory: " + System.getProperty("user.dir"));
        System.out.println("Java version: " + System.getProperty("java.version"));

        try {
            SpringApplication.run(AnalyzerApplication.class, args);
        } catch (Exception e) {
            System.err.println("=== AnalyzerApplication failed ===");
            e.printStackTrace();
            throw e;
        }
    }
}