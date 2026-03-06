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
        System.out.println("🚀🚀🚀 ANALYZER STARTING...");
        System.out.println("Current directory: " + System.getProperty("user.dir"));
        System.out.println("Java version: " + System.getProperty("java.version"));

        try {
            ConfigurableApplicationContext context = SpringApplication.run(AnalyzerApplication.class, args);
            System.out.println("✅✅✅ ANALYZER CONTEXT LOADED");
            System.out.println("Active beans count: " + context.getBeanDefinitionCount());

            HubEventProcessor hubEventProcessor = context.getBean(HubEventProcessor.class);
            System.out.println("🏠🏠🏠 HubEventProcessor bean: " + hubEventProcessor);

            SnapshotProcessor snapshotProcessor = context.getBean(SnapshotProcessor.class);
            System.out.println("📸📸📸 SnapshotProcessor bean: " + snapshotProcessor);

            System.out.println("Starting HubEventProcessor thread...");
            Thread hubEventsThread = new Thread(hubEventProcessor);
            hubEventsThread.setName("HubEventHandlerThread");
            hubEventsThread.start();
            System.out.println("✅ HubEventProcessor thread started");

            System.out.println("Starting SnapshotProcessor...");
            snapshotProcessor.start();

        } catch (Exception e) {
            System.err.println("ANALYZER FAILED TO START");
            e.printStackTrace();
            throw e;
        }
    }
}