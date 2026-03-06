package ru.yandex.practicum;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.context.ConfigurableApplicationContext;
import ru.yandex.practicum.processor.HubEventProcessor;
import ru.yandex.practicum.processor.SnapshotProcessor;

@SpringBootApplication
@ConfigurationPropertiesScan
public class AnalyzerApplication {
    public static void main(String[] args) {
        System.out.println("=== ANALYZER STARTING ===");

        ConfigurableApplicationContext context = SpringApplication.run(AnalyzerApplication.class, args);

        System.out.println("=== CONTEXT LOADED ===");
        System.out.println("Beans count: " + context.getBeanDefinitionCount());

        try {
            HubEventProcessor hubEventProcessor = context.getBean(HubEventProcessor.class);
            System.out.println("HubEventProcessor found: " + hubEventProcessor);

            SnapshotProcessor snapshotProcessor = context.getBean(SnapshotProcessor.class);
            System.out.println("SnapshotProcessor found: " + snapshotProcessor);

            Thread hubEventsThread = new Thread(hubEventProcessor);
            hubEventsThread.setName("HubEventHandlerThread");
            hubEventsThread.start();
            System.out.println("HubEventProcessor thread started");

            snapshotProcessor.start();

        } catch (Exception e) {
            System.err.println("ERROR: " + e.getMessage());
            e.printStackTrace();
        }
    }
}