package ru.yandex.practicum.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;
import ru.yandex.practicum.processor.HubRouterProcessor;
import ru.yandex.practicum.model.Scenario;
import ru.yandex.practicum.model.ScenarioAction;
import ru.yandex.practicum.repository.ScenarioRepository;

import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
public class SnapshotService {

    private final ScenarioRepository scenarioRepository;
    private final HubRouterProcessor hubRouterProcessor;
    private final ScenarioAnalyzerService scenarioAnalyzer;

    public void analyze(SensorsSnapshotAvro snapshot) {
        String hubId = snapshot.getHubId().toString();
        log.info("=== Analyzing snapshot for hub: {} ===", hubId);
        log.info("Snapshot timestamp: {}, sensors count: {}",
                snapshot.getTimestamp(), snapshot.getSensorsState().size());

        List<Scenario> scenarios = scenarioRepository.findByHubIdWithConditionsAndActions(hubId);
        log.info("Found {} scenarios for hub {}", scenarios.size(), hubId);

        if (scenarios.isEmpty()) {
            log.warn("No scenarios found for hub {}", hubId);
            return;
        }

        for (Scenario scenario : scenarios) {
            log.info("Checking scenario: '{}' with {} conditions and {} actions",
                    scenario.getName(),
                    scenario.getConditions().size(),
                    scenario.getActions().size());

            boolean activated = scenarioAnalyzer.checkScenario(scenario, snapshot);
            log.info("Scenario '{}' activated: {}", scenario.getName(), activated);

            if (activated) {
                log.info("✅ Scenario '{}' ACTIVATED for hub {}", scenario.getName(), hubId);

                for (ScenarioAction scenarioAction : scenario.getActions()) {
                    log.info("Executing action: sensor={}, type={}, value={}",
                            scenarioAction.getSensor().getId(),
                            scenarioAction.getAction().getType(),
                            scenarioAction.getAction().getValue());

                    hubRouterProcessor.executeAction(
                            scenarioAction.getAction(),
                            hubId,
                            scenario.getName()
                    );
                }
            } else {
                log.debug("❌ Scenario '{}' not activated", scenario.getName());
            }
        }
    }
}