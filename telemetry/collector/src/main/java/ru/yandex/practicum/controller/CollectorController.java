package ru.yandex.practicum.controller;

import com.google.protobuf.Empty;
import io.grpc.stub.StreamObserver;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.devh.boot.grpc.server.service.GrpcService;
import ru.yandex.practicum.grpc.telemetry.collector.CollectorControllerGrpc;
import ru.yandex.practicum.grpc.telemetry.event.HubEventProto;
import ru.yandex.practicum.grpc.telemetry.event.SensorEventProto;
import ru.yandex.practicum.service.KafkaEventService;

@Slf4j
@GrpcService
@RequiredArgsConstructor
public class CollectorController extends CollectorControllerGrpc.CollectorControllerImplBase {

    private final KafkaEventService kafkaEventService;

    @Override
    public void collectHubEvent(HubEventProto request, StreamObserver<Empty> responseObserver) {
        log.info("Received HubEvent from hub: {}", request.getHubId());

        try {
            log.info("Successfully processed HubEvent");
            responseObserver.onNext(Empty.getDefaultInstance());
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("Error processing HubEvent", e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void collectSensorEvent(SensorEventProto request, StreamObserver<Empty> responseObserver) {
        log.info("Received SensorEvent from sensor: {}", request.getId());

        try {
            log.info("Successfully processed SensorEvent");
            responseObserver.onNext(Empty.getDefaultInstance());
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("Error processing SensorEvent", e);
            responseObserver.onError(e);
        }
    }
}