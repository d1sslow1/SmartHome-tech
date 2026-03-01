CREATE TABLE IF NOT EXISTS scenarios (
                                         id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                                         hub_id VARCHAR NOT NULL,
                                         name VARCHAR NOT NULL,
                                         UNIQUE(hub_id, name)
    );

CREATE TABLE IF NOT EXISTS sensors (
                                       id VARCHAR PRIMARY KEY,
                                       hub_id VARCHAR NOT NULL
);

CREATE TABLE IF NOT EXISTS conditions (
                                          id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                                          type VARCHAR NOT NULL,
                                          operation VARCHAR NOT NULL,
                                          value INTEGER
);

CREATE TABLE IF NOT EXISTS actions (
                                       id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                                       type VARCHAR NOT NULL,
                                       value INTEGER
);

CREATE TABLE IF NOT EXISTS scenario_conditions (
                                                   scenario_id BIGINT REFERENCES scenarios(id) ON DELETE CASCADE,
    sensor_id VARCHAR REFERENCES sensors(id) ON DELETE CASCADE,
    condition_id BIGINT REFERENCES conditions(id) ON DELETE CASCADE,
    PRIMARY KEY (scenario_id, sensor_id, condition_id)
    );

CREATE TABLE IF NOT EXISTS scenario_actions (
                                                scenario_id BIGINT REFERENCES scenarios(id) ON DELETE CASCADE,
    sensor_id VARCHAR REFERENCES sensors(id) ON DELETE CASCADE,
    action_id BIGINT REFERENCES actions(id) ON DELETE CASCADE,
    PRIMARY KEY (scenario_id, sensor_id, action_id)
    );