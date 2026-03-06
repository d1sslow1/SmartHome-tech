package ru.yandex.practicum.processor;

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

    public void start() {
        consumer.subscribe(List.of(snapshotsTopic));
        log.info("SnapshotProcessor subscribed to topic: {}", snapshotsTopic);

        try {
            while (true) {
                ConsumerRecords<String, SensorsSnapshotAvro> records = consumer.poll(Duration.ofMillis(1000));
                log.debug("Received {} snapshot records", records.count());

                for (ConsumerRecord<String, SensorsSnapshotAvro> record : records) {
                    processSnapshot(record.value());
                    currentOffsets.put(
                            new TopicPartition(record.topic(), record.partition()),
                            new OffsetAndMetadata(record.offset() + 1)
                    );
                }

                if (!currentOffsets.isEmpty()) {
                    consumer.commitSync(currentOffsets);
                    currentOffsets.clear();
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
        log.debug("Snapshot timestamp: {}, sensors count: {}",
                snapshot.getTimestamp(), snapshot.getSensorsState().size());

        List<Scenario> scenarios = scenarioRepository.findByHubIdWithConditionsAndActions(hubId);
        log.info("Found {} scenarios for hub {}", scenarios.size(), hubId);

        for (Scenario scenario : scenarios) {
            log.debug("Checking scenario: {} with {} conditions and {} actions",
                    scenario.getName(),
                    scenario.getConditions().size(),
                    scenario.getActions().size());

            if (analyzerService.checkScenario(scenario, snapshot)) {
                log.info("Scenario '{}' activated for hub {}", scenario.getName(), hubId);
                executeActions(hubId, scenario.getName(), scenario.getActions(), snapshot);
            } else {
                log.debug("Scenario '{}' not activated", scenario.getName());
            }
        }
    }

    private void executeActions(String hubId, String scenarioName, List<ScenarioAction> actions, SensorsSnapshotAvro snapshot) {
        log.info("Executing {} actions for scenario '{}' on hub {}", actions.size(), scenarioName, hubId);

        for (ScenarioAction scenarioAction : actions) {
            Action action = scenarioAction.getAction();
            String sensorId = scenarioAction.getSensor().getId();

            log.debug("Sending command: sensor={}, action={}, value={}",
                    sensorId, action.getType(), action.getValue());

            ru.yandex.practicum.grpc.telemetry.event.DeviceActionProto protoAction =
                    ru.yandex.practicum.grpc.telemetry.event.DeviceActionProto.newBuilder()
                            .setSensorId(sensorId)
                            .setType(convertActionType(action.getType()))
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

            try {
                hubRouterClient.handleDeviceAction(request);
                log.info("Successfully sent command to device {}", sensorId);
            } catch (Exception e) {
                log.error("Error sending command to Hub Router for device {}", sensorId, e);
            }
        }
    }

    private ru.yandex.practicum.grpc.telemetry.event.ActionTypeProto convertActionType(ActionType type) {
        return switch (type) {
            case ACTIVATE -> ru.yandex.practicum.grpc.telemetry.event.ActionTypeProto.ACTIVATE;
            case DEACTIVATE -> ru.yandex.practicum.grpc.telemetry.event.ActionTypeProto.DEACTIVATE;
            case INVERSE -> ru.yandex.practicum.grpc.telemetry.event.ActionTypeProto.INVERSE;
            case SET_VALUE -> ru.yandex.practicum.grpc.telemetry.event.ActionTypeProto.SET_VALUE;
        };
    }
}