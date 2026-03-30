package ru.yandex.practicum;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;
import org.springframework.context.ConfigurableApplicationContext;
import ru.yandex.practicum.service.AggregationStarter;

@SpringBootApplication
@ConfigurationPropertiesScan
@EnableDiscoveryClient
public class AggregatorApplication {

    public static void main(String[] args) {
        System.out.println("========================================");
        System.out.println("=== AGGREGATOR APPLICATION STARTING ===");
        System.out.println("========================================");

        ConfigurableApplicationContext context = SpringApplication.run(AggregatorApplication.class, args);

        System.out.println("========================================");
        System.out.println("=== AGGREGATOR CONTEXT LOADED ===");
        System.out.println("Active profiles: " + String.join(", ", context.getEnvironment().getActiveProfiles()));
        System.out.println("========================================");

        AggregationStarter aggregator = context.getBean(AggregationStarter.class);
        System.out.println("=== AGGREGATOR BEAN RETRIEVED: " + aggregator);
        System.out.println("========================================");

        aggregator.start();
        System.out.println("=== AGGREGATOR START METHOD CALLED ===");

        // Бесконечное ожидание, чтобы приложение не завершалось
        try {
            Thread.currentThread().join();
        } catch (InterruptedException e) {
            System.err.println("Aggregator main thread interrupted");
            Thread.currentThread().interrupt();
        }
    }
}