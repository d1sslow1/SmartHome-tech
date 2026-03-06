package ru.yandex.practicum.processor;

import jakarta.annotation.PostConstruct;
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

    @PostConstruct
    public void init() {
        System.out.println("=== HubEventProcessor init() ===");
        System.out.println("hubsTopic: " + hubsTopic);
        System.out.println("consumer: " + consumer);
    }

    @Override
    public void run() {
        System.out.println("=== HubEventProcessor run() ===");
        System.out.println("Subscribing to: " + hubsTopic);

        consumer.subscribe(List.of(hubsTopic));
        System.out.println("Subscribed to: " + hubsTopic);

        try {
            while (true) {
                ConsumerRecords<String, HubEventAvro> records = consumer.poll(Duration.ofMillis(1000));
                System.out.println("Polled " + records.count() + " records");

                for (ConsumerRecord<String, HubEventAvro> record : records) {
                    System.out.println("Processing record: offset=" + record.offset() +
                            ", partition=" + record.partition());
                    processHubEvent(record.value());
                }

                consumer.commitSync();
                System.out.println("Committed offsets");
            }
        } catch (WakeupException e) {
            System.out.println("WakeupException received");
        } catch (Exception e) {
            System.err.println("Error in HubEventProcessor: " + e.getMessage());
            e.printStackTrace();
        } finally {
            consumer.close();
            System.out.println("HubEventProcessor closed");
        }
    }

    @Transactional
    void processHubEvent(HubEventAvro event) {
        String hubId = event.getHubId().toString();
        Object payload = event.getPayload();

        System.out.println("Processing hub event for hub: " + hubId);
        System.out.println("Payload type: " + payload.getClass().getSimpleName());

        if (payload instanceof DeviceAddedEventAvro) {
            processDeviceAdded(hubId, (DeviceAddedEventAvro) payload);
        } else if (payload instanceof DeviceRemovedEventAvro) {
            processDeviceRemoved(hubId, (DeviceRemovedEventAvro) payload);
        } else if (payload instanceof ScenarioAddedEventAvro) {
            processScenarioAdded(hubId, (ScenarioAddedEventAvro) payload);
        } else if (payload instanceof ScenarioRemovedEventAvro) {
            processScenarioRemoved(hubId, (ScenarioRemovedEventAvro) payload);
        }
    }

    private void processDeviceAdded(String hubId, DeviceAddedEventAvro event) {
        String sensorId = event.getId().toString();
        System.out.println("Device added: sensor=" + sensorId + ", hub=" + hubId);

        if (!sensorRepository.existsById(sensorId)) {
            Sensor sensor = new Sensor();
            sensor.setId(sensorId);
            sensor.setHubId(hubId);
            sensorRepository.save(sensor);
            System.out.println("Sensor saved: " + sensorId);
        } else {
            System.out.println("Sensor already exists: " + sensorId);
        }
    }

    private void processDeviceRemoved(String hubId, DeviceRemovedEventAvro event) {
        String sensorId = event.getId().toString();
        System.out.println("Device removed: sensor=" + sensorId + ", hub=" + hubId);

        sensorRepository.findByIdAndHubId(sensorId, hubId).ifPresent(sensor -> {
            sensorRepository.delete(sensor);
            System.out.println("Sensor deleted: " + sensorId);
        });
    }

    private void processScenarioAdded(String hubId, ScenarioAddedEventAvro event) {
        String scenarioName = event.getName().toString();
        System.out.println("Scenario added: name=" + scenarioName + ", hub=" + hubId);

        if (scenarioRepository.findByHubIdAndName(hubId, scenarioName).isEmpty()) {
            createNewScenario(hubId, event);
            System.out.println("Scenario created: " + scenarioName);
        } else {
            System.out.println("Scenario already exists: " + scenarioName);
        }
    }

    @Transactional
    protected void createNewScenario(String hubId, ScenarioAddedEventAvro event) {
        System.out.println("Creating new scenario: " + event.getName());

        Scenario scenario = new Scenario();
        scenario.setHubId(hubId);
        scenario.setName(event.getName().toString());
        final Scenario savedScenario = scenarioRepository.save(scenario);
        System.out.println("Scenario saved with id: " + savedScenario.getId());

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
                System.out.println("Condition created: id=" + savedCondition.getId() +
                        ", sensor=" + currentSensorId);
                conditionCount.incrementAndGet();
            }, () -> {
                System.out.println("Sensor not found: " + currentSensorId + ", condition skipped");
            });
        }
        System.out.println("Total conditions created: " + conditionCount.get());

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
                System.out.println("Action created: id=" + savedAction.getId() +
                        ", sensor=" + currentSensorId);
                actionCount.incrementAndGet();
            }, () -> {
                System.out.println("Sensor not found: " + currentSensorId + ", action skipped");
            });
        }
        System.out.println("Total actions created: " + actionCount.get());
    }

    private void processScenarioRemoved(String hubId, ScenarioRemovedEventAvro event) {
        String scenarioName = event.getName().toString();
        System.out.println("Scenario removed: name=" + scenarioName + ", hub=" + hubId);

        scenarioRepository.findByHubIdAndName(hubId, scenarioName).ifPresent(scenario -> {
            scenarioRepository.delete(scenario);
            System.out.println("Scenario deleted: " + scenarioName);
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
        System.out.println("Unknown value type: " + value.getClass().getSimpleName());
        return null;
    }
}