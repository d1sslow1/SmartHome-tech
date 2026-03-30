package ru.yandex.practicum.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;

import java.time.Duration;
import java.util.List;
import java.util.Optional;

@Slf4j
@Component
@RequiredArgsConstructor
public class AggregationStarter {

    private final KafkaConsumer<String, SensorEventAvro> consumer;
    private final KafkaProducer<String, SensorsSnapshotAvro> producer;
    private final AggregationService aggregationService;

    @Value("${kafka.topics.sensors}")
    private String sensorsTopic;

    @Value("${kafka.topics.snapshots}")
    private String snapshotsTopic;

    public void start() {
        log.info("========================================");
        log.info("=== AggregationStarter START ===");
        log.info("========================================");
        log.info("Consumer instance: {}", consumer);
        log.info("Producer instance: {}", producer);
        log.info("Sensors topic: {}", sensorsTopic);
        log.info("Snapshots topic: {}", snapshotsTopic);
        log.info("========================================");

        try {
            log.info("Subscribing to topic: {}", sensorsTopic);
            consumer.subscribe(List.of(sensorsTopic));
            log.info("✅ Successfully subscribed to topic: {}", sensorsTopic);
        } catch (Exception e) {
            log.error("❌ Failed to subscribe to topic: {}", sensorsTopic, e);
            return;
        }

        try {
            log.info("Starting main processing loop...");
            while (true) {
                log.debug("Polling for messages from Kafka...");
                ConsumerRecords<String, SensorEventAvro> records = consumer.poll(Duration.ofMillis(1000));

                if (records.isEmpty()) {
                    log.debug("No sensor event records received in this poll");
                } else {
                    log.info("📨 Received {} sensor event records", records.count());

                    for (ConsumerRecord<String, SensorEventAvro> record : records) {
                        SensorEventAvro event = record.value();
                        log.info("📊 Processing event: partition={}, offset={}, sensor={}, hub={}, timestamp={}",
                                record.partition(), record.offset(),
                                event.getId(), event.getHubId(), event.getTimestamp());

                        log.debug("Event payload: {}", event.getPayload());

                        Optional<SensorsSnapshotAvro> snapshotOpt = aggregationService.updateState(event);

                        if (snapshotOpt.isPresent()) {
                            SensorsSnapshotAvro snapshot = snapshotOpt.get();
                            log.info("🔄 Snapshot updated for hub: {}, sensors count: {}, timestamp: {}",
                                    snapshot.getHubId(),
                                    snapshot.getSensorsState().size(),
                                    snapshot.getTimestamp());

                            ProducerRecord<String, SensorsSnapshotAvro> producerRecord =
                                    new ProducerRecord<>(snapshotsTopic, snapshot.getHubId().toString(), snapshot);

                            producer.send(producerRecord, (metadata, exception) -> {
                                if (exception != null) {
                                    log.error("❌ Error sending snapshot to Kafka", exception);
                                } else {
                                    log.info("✅ Snapshot sent successfully: topic={}, partition={}, offset={}",
                                            snapshotsTopic, metadata.partition(), metadata.offset());
                                }
                            });

                            log.debug("Snapshot details: {}", snapshot);
                        } else {
                            log.debug("No snapshot update needed for sensor: {}", event.getId());
                        }
                    }
                }

                log.debug("Committing offsets...");
                consumer.commitSync();
                log.debug("Offsets committed successfully");
            }
        } catch (WakeupException e) {
            log.info("⚠️ Received shutdown signal, stopping aggregation...");
        } catch (Exception e) {
            log.error("❌ Error during aggregation", e);
        } finally {
            log.info("========================================");
            log.info("Cleaning up resources...");
            try {
                producer.flush();
                log.info("Producer flushed");
            } catch (Exception e) {
                log.error("Error flushing producer", e);
            } finally {
                try {
                    consumer.close();
                    log.info("Consumer closed");
                } catch (Exception e) {
                    log.error("Error closing consumer", e);
                }
                try {
                    producer.close();
                    log.info("Producer closed");
                } catch (Exception e) {
                    log.error("Error closing producer", e);
                }
            }
            log.info("=== AggregationStarter STOPPED ===");
            log.info("========================================");
        }
    }
}