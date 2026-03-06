package ru.yandex.practicum.processor;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.kafka.telemetry.event.*;
import ru.yandex.practicum.model.*;
import ru.yandex.practicum.repository.*;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
@Component
@RequiredArgsConstructor
public class HubEventProcessor implements Runnable {

    private final KafkaConsumer<String, HubEventAvro> consumer;
    private final SensorRepository sensorRepository;
    private final ScenarioRepository scenarioRepository;
    private final ConditionRepository conditionRepository;
    private final ActionRepository actionRepository;

    @Value("${kafka.topics.hubs}")
    private String hubsTopic;

    @Override
    public void run() {
        log.info("=== HUB EVENT PROCESSOR STARTING ===");
        log.info("Subscribing to topic: {}", hubsTopic);

        consumer.subscribe(List.of(hubsTopic));
        log.info("Subscribed to topic: {}", hubsTopic);

        try {
            while (true) {
                ConsumerRecords<String, HubEventAvro> records = consumer.poll(Duration.ofMillis(1000));

                if (!records.isEmpty()) {
                    log.info("Received {} hub event records", records.count());
                }

                for (ConsumerRecord<String, HubEventAvro> record : records) {
                    log.info("Processing hub event from partition {}, offset {}",
                            record.partition(), record.offset());
                    processHubEvent(record.value());
                }

                consumer.commitSync();
                log.debug("Committed offsets");
            }
        } catch (WakeupException e) {
            log.info("HubEventProcessor received shutdown signal");
        } catch (Exception e) {
            log.error("Error in HubEventProcessor", e);
        } finally {
            consumer.close();
            log.info("HubEventProcessor closed");
        }
    }

    @Transactional
    void processHubEvent(HubEventAvro event) {
        String hubId = event.getHubId().toString();
        Object payload = event.getPayload();

        log.info("Processing hub event for hub: {}, payload type: {}", hubId, payload.getClass().getSimpleName());

        if (payload instanceof DeviceAddedEventAvro) {
            processDeviceAdded(hubId, (DeviceAddedEventAvro) payload);
        } else if (payload instanceof DeviceRemovedEventAvro) {
            processDeviceRemoved(hubId, (DeviceRemovedEventAvro) payload);
        } else if (payload instanceof ScenarioAddedEventAvro) {
            processScenarioAdded(hubId, (ScenarioAddedEventAvro) payload);
        } else if (payload instanceof ScenarioRemovedEventAvro) {
            processScenarioRemoved(hubId, (ScenarioRemovedEventAvro) payload);
        } else {
            log.warn("Unknown payload type: {}", payload.getClass().getSimpleName());
        }
    }

    private void processDeviceAdded(String hubId, DeviceAddedEventAvro event) {
        String sensorId = event.getId().toString();
        log.info("Processing device added: sensor={}, hub={}", sensorId, hubId);

        if (!sensorRepository.existsById(sensorId)) {
            Sensor sensor = new Sensor();
            sensor.setId(sensorId);
            sensor.setHubId(hubId);
            sensorRepository.save(sensor);
            log.info("Added sensor {} for hub {}", sensorId, hubId);
        } else {
            log.info("Sensor {} already exists", sensorId);
        }
    }

    private void processDeviceRemoved(String hubId, DeviceRemovedEventAvro event) {
        String sensorId = event.getId().toString();
        log.info("Processing device removed: sensor={}, hub={}", sensorId, hubId);

        sensorRepository.findByIdAndHubId(sensorId, hubId).ifPresent(sensor -> {
            sensorRepository.delete(sensor);
            log.info("Removed sensor {} from hub {}", sensorId, hubId);
        });
    }

    private void processScenarioAdded(String hubId, ScenarioAddedEventAvro event) {
        String scenarioName = event.getName().toString();
        log.info("Processing scenario added: name={}, hub={}", scenarioName, hubId);

        if (scenarioRepository.findByHubIdAndName(hubId, scenarioName).isEmpty()) {
            createNewScenario(hubId, event);
            log.info("Added scenario {} for hub {}", scenarioName, hubId);
        } else {
            log.info("Scenario {} already exists for hub {}", scenarioName, hubId);
        }
    }

    @Transactional
    protected void createNewScenario(String hubId, ScenarioAddedEventAvro event) {
        log.info("Creating new scenario: {}", event.getName());

        Scenario scenario = new Scenario();
        scenario.setHubId(hubId);
        scenario.setName(event.getName().toString());
        final Scenario savedScenario = scenarioRepository.save(scenario);
        log.info("Created scenario with id: {}", savedScenario.getId());

        AtomicInteger conditionCount = new AtomicInteger(0);
        for (ScenarioConditionAvro conditionAvro : event.getConditions()) {
            String sensorId = conditionAvro.getSensorId().toString();
            final String currentSensorId = sensorId;
            final ScenarioConditionAvro currentConditionAvro = conditionAvro;

            sensorRepository.findByIdAndHubId(currentSensorId, hubId).ifPresentOrElse(sensor -> {
                Condition condition = new Condition();
                condition.setType(mapConditionType(currentConditionAvro.getType()));
                condition.setOperation(mapOperation(currentConditionAvro.getOperation()));
                condition.setValue(extractValue(currentConditionAvro.getValue()));
                Condition savedCondition = conditionRepository.save(condition);
                log.info("Created condition {} for sensor {}", savedCondition.getId(), currentSensorId);
                conditionCount.incrementAndGet();
            }, () -> {
                log.warn("Sensor {} not found for hub {}, condition skipped", currentSensorId, hubId);
            });
        }
        log.info("Processed {} conditions for scenario {}", conditionCount.get(), savedScenario.getName());

        AtomicInteger actionCount = new AtomicInteger(0);
        for (DeviceActionAvro actionAvro : event.getActions()) {
            String sensorId = actionAvro.getSensorId().toString();
            final String currentSensorId = sensorId;
            final DeviceActionAvro currentActionAvro = actionAvro;

            sensorRepository.findByIdAndHubId(currentSensorId, hubId).ifPresentOrElse(sensor -> {
                Action action = new Action();
                action.setType(mapActionType(currentActionAvro.getType()));
                action.setValue((Integer) currentActionAvro.getValue());
                Action savedAction = actionRepository.save(action);
                log.info("Created action {} for sensor {}", savedAction.getId(), currentSensorId);
                actionCount.incrementAndGet();
            }, () -> {
                log.warn("Sensor {} not found for hub {}, action skipped", currentSensorId, hubId);
            });
        }
        log.info("Processed {} actions for scenario {}", actionCount.get(), savedScenario.getName());

        log.info("Scenario creation completed for: {}", event.getName());
    }

    private void processScenarioRemoved(String hubId, ScenarioRemovedEventAvro event) {
        String scenarioName = event.getName().toString();
        log.info("Processing scenario removed: name={}, hub={}", scenarioName, hubId);

        scenarioRepository.findByHubIdAndName(hubId, scenarioName).ifPresent(scenario -> {
            scenarioRepository.delete(scenario);
            log.info("Removed scenario {} from hub {}", scenarioName, hubId);
        });
    }

    private ConditionType mapConditionType(ConditionTypeAvro type) {
        switch (type) {
            case MOTION: return ConditionType.MOTION;
            case LUMINOSITY: return ConditionType.LUMINOSITY;
            case SWITCH: return ConditionType.SWITCH;
            case TEMPERATURE: return ConditionType.TEMPERATURE;
            case CO2LEVEL: return ConditionType.CO2LEVEL;
            case HUMIDITY: return ConditionType.HUMIDITY;
            default: throw new IllegalArgumentException("Unknown condition type: " + type);
        }
    }

    private ConditionOperation mapOperation(ConditionOperationAvro op) {
        switch (op) {
            case EQUALS: return ConditionOperation.EQUALS;
            case GREATER_THAN: return ConditionOperation.GREATER_THAN;
            case LOWER_THAN: return ConditionOperation.LOWER_THAN;
            default: throw new IllegalArgumentException("Unknown operation: " + op);
        }
    }

    private ActionType mapActionType(ActionTypeAvro type) {
        switch (type) {
            case ACTIVATE: return ActionType.ACTIVATE;
            case DEACTIVATE: return ActionType.DEACTIVATE;
            case INVERSE: return ActionType.INVERSE;
            case SET_VALUE: return ActionType.SET_VALUE;
            default: throw new IllegalArgumentException("Unknown action type: " + type);
        }
    }

    private Integer extractValue(Object value) {
        if (value == null) return null;
        if (value instanceof Integer) return (Integer) value;
        if (value instanceof Boolean) return ((Boolean) value) ? 1 : 0;
        log.warn("Unknown value type: {}", value.getClass().getSimpleName());
        return null;
    }
}