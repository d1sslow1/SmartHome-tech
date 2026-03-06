package ru.yandex.practicum.processor;

import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.devh.boot.grpc.client.inject.GrpcClient;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.grpc.telemetry.hubrouter.HubRouterControllerGrpc;
import ru.yandex.practicum.grpc.telemetry.hubrouter.DeviceActionRequest;
import ru.yandex.practicum.kafka.telemetry.event.*;
import ru.yandex.practicum.model.*;
import ru.yandex.practicum.repository.ScenarioRepository;
import ru.yandex.practicum.service.ScenarioAnalyzerService;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@Component
@RequiredArgsConstructor
public class SnapshotProcessor {
    private final KafkaConsumer<String, SensorsSnapshotAvro> consumer;
    private final ScenarioRepository scenarioRepository;
    private final ScenarioAnalyzerService analyzerService;

    @GrpcClient("hub-router")
    private HubRouterControllerGrpc.HubRouterControllerBlockingStub hubRouterClient;

    @Value("${kafka.topics.snapshots}")
    private String snapshotsTopic;

    private final Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();

    @PostConstruct
    public void init() {
        log.info("SnapshotProcessor bean created");
        log.info("Kafka consumer: {}", consumer);
        log.info("Topic: {}", snapshotsTopic);
        log.info("HubRouterClient: {}", hubRouterClient);
    }

    public void start() {
        log.info("SnapshotProcessor starting");
        log.info("Subscribing to topic: {}", snapshotsTopic);

        consumer.subscribe(List.of(snapshotsTopic));
        log.info("Subscribed to topic: {}", snapshotsTopic);

        try {
            while (true) {
                log.debug("Polling Kafka...");
                ConsumerRecords<String, SensorsSnapshotAvro> records = consumer.poll(Duration.ofMillis(1000));
                log.debug("Poll returned {} records", records.count());

                if (!records.isEmpty()) {
                    log.info("Received {} snapshot records", records.count());
                }

                for (ConsumerRecord<String, SensorsSnapshotAvro> record : records) {
                    log.info("Processing snapshot record from partition {}, offset {}",
                            record.partition(), record.offset());
                    processSnapshot(record.value());
                    currentOffsets.put(
                            new TopicPartition(record.topic(), record.partition()),
                            new OffsetAndMetadata(record.offset() + 1)
                    );
                }

                if (!currentOffsets.isEmpty()) {
                    consumer.commitSync(currentOffsets);
                    currentOffsets.clear();
                    log.debug("Committed offsets");
                }
            }
        } catch (WakeupException e) {
            log.info("SnapshotProcessor received shutdown signal");
        } catch (Exception e) {
            log.error("Error in SnapshotProcessor", e);
        } finally {
            consumer.close();
            log.info("SnapshotProcessor closed");
        }
    }

    private void processSnapshot(SensorsSnapshotAvro snapshot) {
        String hubId = snapshot.getHubId().toString();
        log.info("Processing snapshot for hub: {}", hubId);
        log.info("Snapshot timestamp: {}, sensors count: {}",
                snapshot.getTimestamp(), snapshot.getSensorsState().size());

        List<Scenario> scenarios = scenarioRepository.findByHubIdWithConditionsAndActions(hubId);
        log.info("Found {} scenarios for hub {}", scenarios.size(), hubId);

        if (scenarios.isEmpty()) {
            log.warn("No scenarios found for hub {}", hubId);
            return;
        }

        for (Scenario scenario : scenarios) {
            log.info("Checking scenario: {} with {} conditions and {} actions",
                    scenario.getName(),
                    scenario.getConditions().size(),
                    scenario.getActions().size());

            boolean scenarioResult = analyzerService.checkScenario(scenario, snapshot);
            log.info("Scenario {} check result: {}", scenario.getName(), scenarioResult);

            if (scenarioResult) {
                log.info("Scenario {} activated for hub {}", scenario.getName(), hubId);
                executeActions(hubId, scenario.getName(), scenario.getActions(), snapshot);
            } else {
                log.info("Scenario {} not activated", scenario.getName());
            }
        }
    }

    private void executeActions(String hubId, String scenarioName, List<ScenarioAction> actions, SensorsSnapshotAvro snapshot) {
        log.info("Executing {} actions for scenario {} on hub {}", actions.size(), scenarioName, hubId);

        if (actions.isEmpty()) {
            log.warn("No actions to execute for scenario {}", scenarioName);
            return;
        }

        for (ScenarioAction scenarioAction : actions) {
            if (scenarioAction == null || scenarioAction.getAction() == null || scenarioAction.getSensor() == null) {
                log.error("Invalid scenario action for scenario {}", scenarioName);
                continue;
            }

            Action action = scenarioAction.getAction();
            String sensorId = scenarioAction.getSensor().getId();

            log.info("Sending command: sensor={}, action={}, value={}",
                    sensorId, action.getType(), action.getValue());

            ru.yandex.practicum.grpc.telemetry.event.ActionTypeProto actionType;
            try {
                actionType = convertActionType(action.getType());
            } catch (Exception e) {
                log.error("Failed to convert action type: {}", action.getType(), e);
                continue;
            }

            ru.yandex.practicum.grpc.telemetry.event.DeviceActionProto protoAction =
                    ru.yandex.practicum.grpc.telemetry.event.DeviceActionProto.newBuilder()
                            .setSensorId(sensorId)
                            .setType(actionType)
                            .setValue(action.getValue() != null ? action.getValue() : 0)
                            .build();

            DeviceActionRequest request = DeviceActionRequest.newBuilder()
                    .setHubId(hubId)
                    .setScenarioName(scenarioName)
                    .setAction(protoAction)
                    .setTimestamp(com.google.protobuf.Timestamp.newBuilder()
                            .setSeconds(Instant.now().getEpochSecond())
                            .setNanos(Instant.now().getNano())
                            .build())
                    .build();

            log.info("Sending gRPC request to hub-router: hubId={}, scenario={}, sensor={}",
                    hubId, scenarioName, sensorId);

            try {
                hubRouterClient.handleDeviceAction(request);
                log.info("Successfully sent command to device {}", sensorId);
            } catch (Exception e) {
                log.error("Error sending command to Hub Router for device {}", sensorId, e);
            }
        }
    }

    private ru.yandex.practicum.grpc.telemetry.event.ActionTypeProto convertActionType(ActionType type) {
        switch (type) {
            case ACTIVATE:
                return ru.yandex.practicum.grpc.telemetry.event.ActionTypeProto.ACTIVATE;
            case DEACTIVATE:
                return ru.yandex.practicum.grpc.telemetry.event.ActionTypeProto.DEACTIVATE;
            case INVERSE:
                return ru.yandex.practicum.grpc.telemetry.event.ActionTypeProto.INVERSE;
            case SET_VALUE:
                return ru.yandex.practicum.grpc.telemetry.event.ActionTypeProto.SET_VALUE;
            default:
                log.error("Unknown action type: {}", type);
                throw new IllegalArgumentException("Unknown action type: " + type);
        }
    }
}