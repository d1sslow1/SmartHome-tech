package ru.yandex.practicum.model;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.Setter;

@Entity
@Table(name = "actions")
@Getter
@Setter
public class Action {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private ActionType type;

    private Integer value;

    @OneToOne(mappedBy = "action")
    private ScenarioAction scenarioAction;

    public Sensor getSensor() {
        return scenarioAction != null ? scenarioAction.getSensor() : null;
    }

    public String getSensorId() {
        Sensor sensor = getSensor();
        return sensor != null ? sensor.getId() : null;
    }
}