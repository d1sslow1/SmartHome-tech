package ru.yandex.practicum.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import ru.yandex.practicum.model.Scenario;
import java.util.List;
import java.util.Optional;

public interface ScenarioRepository extends JpaRepository<Scenario, Long> {
    List<Scenario> findByHubId(String hubId);
    Optional<Scenario> findByHubIdAndName(String hubId, String name);
    @Query("SELECT s FROM Scenario s LEFT JOIN FETCH s.conditions LEFT JOIN FETCH s.actions WHERE s.hubId = :hubId")
    List<Scenario> findByHubIdWithConditionsAndActions(String hubId);
}