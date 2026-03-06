package ru.yandex.practicum.controller;

import com.google.protobuf.Empty;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;
import ru.yandex.practicum.grpc.telemetry.collector.CollectorControllerGrpc;
import ru.yandex.practicum.grpc.telemetry.event.HubEventProto;
import ru.yandex.practicum.grpc.telemetry.event.SensorEventProto;

@GrpcService
public class CollectorController extends CollectorControllerGrpc.CollectorControllerImplBase {

    @Override
    public void collectHubEvent(HubEventProto request, StreamObserver<Empty> responseObserver) {
        System.out.println("Received HubEvent from hub: " + request.getHubId());
        System.out.println("Event payload: " + request.getPayloadCase());

        responseObserver.onNext(Empty.getDefaultInstance());
        responseObserver.onCompleted();
    }

    @Override
    public void collectSensorEvent(SensorEventProto request, StreamObserver<Empty> responseObserver) {
        System.out.println("Received SensorEvent from sensor: " + request.getId());
        System.out.println("Hub ID: " + request.getHubId());
        System.out.println("Event payload: " + request.getPayloadCase());
        responseObserver.onNext(Empty.getDefaultInstance());
        responseObserver.onCompleted();
    }
}