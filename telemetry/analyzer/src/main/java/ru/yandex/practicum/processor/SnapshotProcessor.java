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
        System.out.println("=== SnapshotProcessor.init() START ===");
        System.out.println("SnapshotProcessor bean created");
        System.out.println("Kafka consumer: " + consumer);
        System.out.println("Topic: " + snapshotsTopic);
        System.out.println("HubRouterClient: " + hubRouterClient);
        System.out.println("=== SnapshotProcessor.init() END ===");
        System.out.flush();
    }

    public void start() {
        System.out.println("=== SnapshotProcessor.start() START ===");
        System.out.println("Subscribing to topic: " + snapshotsTopic);

        consumer.subscribe(List.of(snapshotsTopic));
        System.out.println("Subscribed to topic: " + snapshotsTopic);

        try {
            while (true) {
                System.out.println("Polling Kafka...");
                ConsumerRecords<String, SensorsSnapshotAvro> records = consumer.poll(Duration.ofMillis(1000));
                System.out.println("Poll returned " + records.count() + " records");

                if (!records.isEmpty()) {
                    System.out.println("Received " + records.count() + " snapshot records");
                }

                for (ConsumerRecord<String, SensorsSnapshotAvro> record : records) {
                    System.out.println("Processing snapshot record from partition " + record.partition() +
                            ", offset " + record.offset());
                    processSnapshot(record.value());
                    currentOffsets.put(
                            new TopicPartition(record.topic(), record.partition()),
                            new OffsetAndMetadata(record.offset() + 1)
                    );
                }

                if (!currentOffsets.isEmpty()) {
                    consumer.commitSync(currentOffsets);
                    currentOffsets.clear();
                    System.out.println("Committed offsets");
                }
            }
        } catch (WakeupException e) {
            System.out.println("SnapshotProcessor received shutdown signal");
        } catch (Exception e) {
            System.err.println("Error in SnapshotProcessor: " + e.getMessage());
            e.printStackTrace();
        } finally {
            consumer.close();
            System.out.println("SnapshotProcessor closed");
        }
    }

    private void processSnapshot(SensorsSnapshotAvro snapshot) {
        String hubId = snapshot.getHubId().toString();
        System.out.println("=== PROCESSING SNAPSHOT FOR HUB: " + hubId + " ===");
        System.out.println("Snapshot timestamp: " + snapshot.getTimestamp() +
                ", sensors count: " + snapshot.getSensorsState().size());

        List<Scenario> scenarios = scenarioRepository.findByHubIdWithConditionsAndActions(hubId);
        System.out.println("Found " + scenarios.size() + " scenarios for hub " + hubId);

        if (scenarios.isEmpty()) {
            System.out.println("No scenarios found for hub " + hubId);
            return;
        }

        for (Scenario scenario : scenarios) {
            System.out.println("Checking scenario: " + scenario.getName() +
                    " with " + scenario.getConditions().size() + " conditions");

            boolean scenarioResult = analyzerService.checkScenario(scenario, snapshot);
            System.out.println("Scenario " + scenario.getName() + " check result: " + scenarioResult);

            if (scenarioResult) {
                System.out.println("Scenario " + scenario.getName() + " activated for hub " + hubId);
                executeActions(hubId, scenario.getName(), scenario.getActions(), snapshot);
            } else {
                System.out.println("Scenario " + scenario.getName() + " not activated");
            }
        }
    }

    private void executeActions(String hubId, String scenarioName, List<ScenarioAction> actions, SensorsSnapshotAvro snapshot) {
        System.out.println("Executing " + actions.size() + " actions for scenario " + scenarioName + " on hub " + hubId);

        if (actions.isEmpty()) {
            System.out.println("No actions to execute for scenario " + scenarioName);
            return;
        }

        for (ScenarioAction scenarioAction : actions) {
            if (scenarioAction == null || scenarioAction.getAction() == null || scenarioAction.getSensor() == null) {
                System.err.println("Invalid scenario action for scenario " + scenarioName);
                continue;
            }

            Action action = scenarioAction.getAction();
            String sensorId = scenarioAction.getSensor().getId();

            System.out.println("Sending command: sensor=" + sensorId +
                    ", action=" + action.getType() +
                    ", value=" + action.getValue());

            ru.yandex.practicum.grpc.telemetry.event.ActionTypeProto actionType;
            try {
                actionType = convertActionType(action.getType());
            } catch (Exception e) {
                System.err.println("Failed to convert action type: " + action.getType());
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

            System.out.println("Sending gRPC request to hub-router");

            try {
                hubRouterClient.handleDeviceAction(request);
                System.out.println("Successfully sent command to device " + sensorId);
            } catch (Exception e) {
                System.err.println("Error sending command to Hub Router for device " + sensorId + ": " + e.getMessage());
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
                System.err.println("Unknown action type: " + type);
                throw new IllegalArgumentException("Unknown action type: " + type);
        }
    }
}