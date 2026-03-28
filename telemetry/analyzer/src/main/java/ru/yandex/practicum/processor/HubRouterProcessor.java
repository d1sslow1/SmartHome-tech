package ru.yandex.practicum.processor;

import com.google.protobuf.Empty;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.grpc.telemetry.event.ActionTypeProto;
import ru.yandex.practicum.grpc.telemetry.event.DeviceActionProto;
import ru.yandex.practicum.grpc.telemetry.hubrouter.DeviceActionRequest;
import ru.yandex.practicum.grpc.telemetry.hubrouter.HubRouterControllerGrpc;
import ru.yandex.practicum.model.Action;
import ru.yandex.practicum.model.Sensor;

@Slf4j
@Service
public class HubRouterProcessor {

    private final HubRouterControllerGrpc.HubRouterControllerBlockingStub hubRouterClient;

    public HubRouterProcessor(@Value("${grpc.client.hub-router.address}") String address) {
        log.info("Creating HubRouterProcessor with address: {}", address);
        ManagedChannel channel = ManagedChannelBuilder.forTarget(address)
                .usePlaintext()
                .build();
        this.hubRouterClient = HubRouterControllerGrpc.newBlockingStub(channel);
        log.info("HubRouterProcessor created successfully");
    }

    @PostConstruct
    public void init() {
        log.info("=== HubRouterProcessor INIT ===");
    }

    public Empty executeAction(Action action, String hubId, String scenarioName) {
        log.info("=== executeAction called ===");
        log.info("hubId={}, scenario={}, actionId={}", hubId, scenarioName, action.getId());

        Sensor sensor = action.getSensor();
        if (sensor == null) {
            log.error("Action has no associated sensor: {}", action.getId());
            return null;
        }

        log.info("Sending command: sensor={}, type={}, value={}",
                sensor.getId(), action.getType(), action.getValue());

        DeviceActionProto deviceActionProto = DeviceActionProto.newBuilder()
                .setSensorId(sensor.getId())
                .setType(ActionTypeProto.valueOf(action.getType().name()))
                .setValue(action.getValue() != null ? action.getValue() : 0)
                .build();

        DeviceActionRequest request = DeviceActionRequest.newBuilder()
                .setHubId(hubId)
                .setScenarioName(scenarioName)
                .setAction(deviceActionProto)
                .build();

        try {
            log.debug("Sending gRPC request to hub-router");
            Empty response = hubRouterClient.handleDeviceAction(request);
            log.info("✅ Action executed successfully: sensor={}, type={}, value={}",
                    sensor.getId(), action.getType(), action.getValue());
            return response;
        } catch (StatusRuntimeException e) {
            log.error("❌ Error sending DeviceActionRequest: {}", e.getMessage(), e);
            return null;
        }
    }
}