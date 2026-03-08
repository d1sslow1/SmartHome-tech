package ru.yandex.practicum.processor;

import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;
import ru.yandex.practicum.config.KafkaConfig;
import ru.yandex.practicum.service.SnapshotService;

import java.time.Duration;
import java.util.List;

@Slf4j
@Component
public class SnapshotProcessor implements Runnable {

    private final KafkaConsumer<String, SensorsSnapshotAvro> snapshotConsumer;
    private final KafkaConfig kafkaConfig;
    private final SnapshotService snapshotService;

    public SnapshotProcessor(KafkaConsumer<String, SensorsSnapshotAvro> snapshotConsumer,
                             KafkaConfig kafkaConfig,
                             SnapshotService snapshotService) {
        this.snapshotConsumer = snapshotConsumer;
        this.kafkaConfig = kafkaConfig;
        this.snapshotService = snapshotService;
    }

    @PostConstruct
    public void init() {
        log.info("=== SnapshotProcessor INIT ===");
        log.info("Topic: {}", kafkaConfig.getSnapshotsTopic());
    }

    public void start() {
        run();
    }

    @Override
    public void run() {
        log.info("=== SnapshotProcessor START ===");
        log.info("Subscribing to: {}", kafkaConfig.getSnapshotsTopic());

        try {
            Runtime.getRuntime().addShutdownHook(new Thread(snapshotConsumer::wakeup));
            snapshotConsumer.subscribe(List.of(kafkaConfig.getSnapshotsTopic()));

            while (true) {
                ConsumerRecords<String, SensorsSnapshotAvro> records = snapshotConsumer.poll(Duration.ofSeconds(5));

                if (!records.isEmpty()) {
                    log.info("Received {} snapshot records", records.count());
                }

                for (ConsumerRecord<String, SensorsSnapshotAvro> record : records) {
                    log.info("Processing snapshot from partition {}, offset {}",
                            record.partition(), record.offset());
                    SensorsSnapshotAvro sensorsSnapshotAvro = record.value();
                    snapshotService.analyze(sensorsSnapshotAvro);
                }

                snapshotConsumer.commitAsync((offsets, exception) -> {
                    if (exception != null) {
                        log.warn("Commit snapshot processing error. Offsets: {}", offsets, exception);
                    }
                });
            }
        } catch (WakeupException ignored) {
            log.info("SnapshotProcessor wakeup");
        } catch (Exception e) {
            log.error("Error handling SnapshotEvents from kafka", e);
        } finally {
            snapshotConsumer.close();
            log.info("SnapshotProcessor closed");
        }
    }
}