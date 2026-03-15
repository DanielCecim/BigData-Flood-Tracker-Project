CREATE TABLE IF NOT EXISTS weather_snapshots (
    id                 SERIAL PRIMARY KEY,
    flood_event_id     INTEGER REFERENCES flood_events(id),
    polled_at          TIMESTAMPTZ DEFAULT NOW(),
    temperature_c      NUMERIC(5,2),
    precipitation_mm   NUMERIC(6,2),
    wind_speed_kmh     NUMERIC(6,2),
    wind_direction_deg INTEGER,
    weather_code       INTEGER,
    humidity_pct       INTEGER
);

ALTER TABLE weather_snapshots
ADD CONSTRAINT uq_weather_snapshot
UNIQUE (flood_event_id, polled_at);
