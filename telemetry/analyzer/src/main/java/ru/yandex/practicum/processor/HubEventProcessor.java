package ru.yandex.practicum.processor;

import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.event.*;
import ru.yandex.practicum.config.KafkaConfig;
import ru.yandex.practicum.model.Sensor;
import ru.yandex.practicum.model.Scenario;
import ru.yandex.practicum.repository.SensorRepository;
import ru.yandex.practicum.repository.ScenarioRepository;

import java.time.Duration;
import java.util.List;

@Slf4j
@Component
public class HubEventProcessor implements Runnable {

    private final KafkaConsumer<String, HubEventAvro> hubConsumer;
    private final KafkaConfig kafkaConfig;
    private final SensorRepository sensorRepository;
    private final ScenarioRepository scenarioRepository;

    public HubEventProcessor(KafkaConsumer<String, HubEventAvro> hubConsumer,
                             KafkaConfig kafkaConfig,
                             SensorRepository sensorRepository,
                             ScenarioRepository scenarioRepository) {
        this.hubConsumer = hubConsumer;
        this.kafkaConfig = kafkaConfig;
        this.sensorRepository = sensorRepository;
        this.scenarioRepository = scenarioRepository;
    }

    @PostConstruct
    public void init() {
        log.info("=== HubEventProcessor INIT ===");
        log.info("Topic: {}", kafkaConfig.getHubsTopic());
    }

    @Override
    public void run() {
        log.info("=== HubEventProcessor START ===");
        log.info("Subscribing to: {}", kafkaConfig.getHubsTopic());

        try {
            Runtime.getRuntime().addShutdownHook(new Thread(hubConsumer::wakeup));
            hubConsumer.subscribe(List.of(kafkaConfig.getHubsTopic()));

            while (true) {
                ConsumerRecords<String, HubEventAvro> records = hubConsumer.poll(Duration.ofSeconds(5));

                if (!records.isEmpty()) {
                    log.info("Received {} hub event records", records.count());
                }

                for (ConsumerRecord<String, HubEventAvro> record : records) {
                    log.info("Processing hub event from partition {}, offset {}",
                            record.partition(), record.offset());
                    HubEventAvro hubEventAvro = record.value();
                    processHubEvent(hubEventAvro);
                }

                hubConsumer.commitAsync((offsets, exception) -> {
                    if (exception != null) {
                        log.warn("Commit hubEvent processing error. Offsets: {}", offsets, exception);
                    }
                });
            }
        } catch (WakeupException ignored) {
            log.info("HubEventProcessor wakeup");
        } catch (Exception e) {
            log.error("Error handling HubEvents from kafka", e);
        } finally {
            hubConsumer.close();
            log.info("HubEventProcessor closed");
        }
    }

    private void processHubEvent(HubEventAvro event) {
        String hubId = event.getHubId().toString();
        Object payload = event.getPayload();

        log.info("Processing hub event for hub: {}, payload type: {}",
                hubId, payload.getClass().getSimpleName());

        if (payload instanceof DeviceAddedEventAvro deviceAddedEventAvro) {
            Sensor sensor = new Sensor();
            sensor.setId(deviceAddedEventAvro.getId().toString());
            sensor.setHubId(hubId);
            sensorRepository.save(sensor);
            log.info("Added sensor {} for hub {}", sensor.getId(), hubId);

        } else if (payload instanceof DeviceRemovedEventAvro deviceRemovedEventAvro) {
            sensorRepository.deleteById(deviceRemovedEventAvro.getId().toString());
            log.info("Removed sensor {} from hub {}", deviceRemovedEventAvro.getId(), hubId);

        } else if (payload instanceof ScenarioAddedEventAvro scenarioAddedEventAvro) {
            Scenario scenario = new Scenario();
            scenario.setHubId(hubId);
            scenario.setName(scenarioAddedEventAvro.getName().toString());
            scenarioRepository.save(scenario);
            log.info("Added scenario {} for hub {}", scenario.getName(), hubId);

        } else if (payload instanceof ScenarioRemovedEventAvro scenarioRemovedEventAvro) {
            String scenarioName = scenarioRemovedEventAvro.getName().toString();
            scenarioRepository.findByHubIdAndName(hubId, scenarioName)
                    .ifPresent(scenario -> scenarioRepository.delete(scenario));
            log.info("Removed scenario {} from hub {}", scenarioName, hubId);

        } else {
            log.warn("Unknown payload type: {}", payload.getClass().getSimpleName());
        }
    }
}