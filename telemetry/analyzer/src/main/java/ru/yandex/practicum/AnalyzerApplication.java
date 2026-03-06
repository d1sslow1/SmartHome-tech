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
        System.out.println("=== ANALYZER MAIN START ===");
        System.out.println("Current directory: " + System.getProperty("user.dir"));
        System.out.println("Java version: " + System.getProperty("java.version"));
        System.out.flush();

        try {
            ConfigurableApplicationContext context = SpringApplication.run(AnalyzerApplication.class, args);
            System.out.println("=== CONTEXT LOADED ===");
            System.out.println("Beans count: " + context.getBeanDefinitionCount());
            System.out.flush();

            String[] beanNames = context.getBeanDefinitionNames();
            System.out.println("Bean names sample:");
            for (int i = 0; i < Math.min(10, beanNames.length); i++) {
                System.out.println("  - " + beanNames[i]);
            }
            System.out.flush();

            System.out.println("Looking for HubEventProcessor...");
            HubEventProcessor hubEventProcessor = null;
            try {
                hubEventProcessor = context.getBean(HubEventProcessor.class);
                System.out.println("FOUND HubEventProcessor: " + hubEventProcessor);
            } catch (Exception e) {
                System.err.println("HubEventProcessor NOT FOUND: " + e.getMessage());
                System.err.flush();
            }

            System.out.println("Looking for SnapshotProcessor...");
            SnapshotProcessor snapshotProcessor = null;
            try {
                snapshotProcessor = context.getBean(SnapshotProcessor.class);
                System.out.println("FOUND SnapshotProcessor: " + snapshotProcessor);
            } catch (Exception e) {
                System.err.println("SnapshotProcessor NOT FOUND: " + e.getMessage());
                System.err.flush();
            }

            if (hubEventProcessor != null) {
                System.out.println("Starting HubEventProcessor thread...");
                Thread hubEventsThread = new Thread(hubEventProcessor);
                hubEventsThread.setName("HubEventHandlerThread");
                hubEventsThread.start();
                System.out.println("HubEventProcessor thread started");
            } else {
                System.err.println("HubEventProcessor is null, cannot start thread");
            }

            if (snapshotProcessor != null) {
                System.out.println("Starting SnapshotProcessor...");
                snapshotProcessor.start();
            } else {
                System.err.println("SnapshotProcessor is null, cannot start");
            }

        } catch (Exception e) {
            System.err.println("=== ANALYZER FAILED ===");
            e.printStackTrace();
            System.err.flush();
            throw e;
        }
    }
}