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
        System.out.println("=== HUB EVENT PROCESSOR INIT START ===");
        System.out.println("HubEventProcessor bean created");
        System.out.println("Kafka consumer: " + consumer);
        System.out.println("Topic: " + hubsTopic);
        System.out.println("SensorRepository: " + sensorRepository);
        System.out.println("ScenarioRepository: " + scenarioRepository);
        System.out.println("=== HUB EVENT PROCESSOR INIT END ===");
        System.out.flush();
    }

    @Override
    public void run() {
        System.out.println("=== HUB EVENT PROCESSOR RUN START ===");
        System.out.println("Subscribing to topic: " + hubsTopic);

        consumer.subscribe(List.of(hubsTopic));
        System.out.println("Subscribed to topic: " + hubsTopic);
        System.out.flush();

        try {
            while (true) {
                System.out.println("Polling Kafka for hub events...");
                ConsumerRecords<String, HubEventAvro> records = consumer.poll(Duration.ofMillis(1000));
                System.out.println("Poll returned " + records.count() + " hub event records");

                if (!records.isEmpty()) {
                    System.out.println("Received " + records.count() + " hub event records");
                }

                for (ConsumerRecord<String, HubEventAvro> record : records) {
                    System.out.println("Processing hub event from partition " + record.partition() +
                            ", offset " + record.offset() +
                            ", key: " + record.key());
                    processHubEvent(record.value());
                }

                consumer.commitSync();
                System.out.println("Committed offsets");
            }
        } catch (WakeupException e) {
            System.out.println("HubEventProcessor received shutdown signal");
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

        System.out.println("=== PROCESSING HUB EVENT ===");
        System.out.println("Hub ID: " + hubId);
        System.out.println("Payload type: " + payload.getClass().getSimpleName());
        System.out.flush();

        if (payload instanceof DeviceAddedEventAvro) {
            processDeviceAdded(hubId, (DeviceAddedEventAvro) payload);
        } else if (payload instanceof DeviceRemovedEventAvro) {
            processDeviceRemoved(hubId, (DeviceRemovedEventAvro) payload);
        } else if (payload instanceof ScenarioAddedEventAvro) {
            processScenarioAdded(hubId, (ScenarioAddedEventAvro) payload);
        } else if (payload instanceof ScenarioRemovedEventAvro) {
            processScenarioRemoved(hubId, (ScenarioRemovedEventAvro) payload);
        } else {
            System.out.println("Unknown payload type: " + payload.getClass().getSimpleName());
        }
    }

    private void processDeviceAdded(String hubId, DeviceAddedEventAvro event) {
        String sensorId = event.getId().toString();
        System.out.println("Processing device added: sensor=" + sensorId + ", hub=" + hubId);

        if (!sensorRepository.existsById(sensorId)) {
            Sensor sensor = new Sensor();
            sensor.setId(sensorId);
            sensor.setHubId(hubId);
            sensorRepository.save(sensor);
            System.out.println("Added sensor " + sensorId + " for hub " + hubId);
        } else {
            System.out.println("Sensor " + sensorId + " already exists");
        }
        System.out.flush();
    }

    private void processDeviceRemoved(String hubId, DeviceRemovedEventAvro event) {
        String sensorId = event.getId().toString();
        System.out.println("Processing device removed: sensor=" + sensorId + ", hub=" + hubId);

        sensorRepository.findByIdAndHubId(sensorId, hubId).ifPresent(sensor -> {
            sensorRepository.delete(sensor);
            System.out.println("Removed sensor " + sensorId + " from hub " + hubId);
        });
        System.out.flush();
    }

    private void processScenarioAdded(String hubId, ScenarioAddedEventAvro event) {
        String scenarioName = event.getName().toString();
        System.out.println("Processing scenario added: name=" + scenarioName + ", hub=" + hubId);

        if (scenarioRepository.findByHubIdAndName(hubId, scenarioName).isEmpty()) {
            createNewScenario(hubId, event);
            System.out.println("Added scenario " + scenarioName + " for hub " + hubId);
        } else {
            System.out.println("Scenario " + scenarioName + " already exists for hub " + hubId);
        }
        System.out.flush();
    }

    @Transactional
    protected void createNewScenario(String hubId, ScenarioAddedEventAvro event) {
        System.out.println("Creating new scenario: " + event.getName());

        Scenario scenario = new Scenario();
        scenario.setHubId(hubId);
        scenario.setName(event.getName().toString());
        final Scenario savedScenario = scenarioRepository.save(scenario);
        System.out.println("Created scenario with id: " + savedScenario.getId());

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
                System.out.println("Created condition " + savedCondition.getId() + " for sensor " + currentSensorId);
                conditionCount.incrementAndGet();
            }, () -> {
                System.out.println("Sensor " + currentSensorId + " not found for hub " + hubId + ", condition skipped");
            });
        }
        System.out.println("Processed " + conditionCount.get() + " conditions for scenario " + savedScenario.getName());

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
                System.out.println("Created action " + savedAction.getId() + " for sensor " + currentSensorId);
                actionCount.incrementAndGet();
            }, () -> {
                System.out.println("Sensor " + currentSensorId + " not found for hub " + hubId + ", action skipped");
            });
        }
        System.out.println("Processed " + actionCount.get() + " actions for scenario " + savedScenario.getName());

        System.out.println("Scenario creation completed for: " + event.getName());
        System.out.flush();
    }

    private void processScenarioRemoved(String hubId, ScenarioRemovedEventAvro event) {
        String scenarioName = event.getName().toString();
        System.out.println("Processing scenario removed: name=" + scenarioName + ", hub=" + hubId);

        scenarioRepository.findByHubIdAndName(hubId, scenarioName).ifPresent(scenario -> {
            scenarioRepository.delete(scenario);
            System.out.println("Removed scenario " + scenarioName + " from hub " + hubId);
        });
        System.out.flush();
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