CREATE TABLE IF NOT EXISTS station_readings (
    id            SERIAL PRIMARY KEY,
    station_ref   TEXT,
    station_label TEXT,
    lat           NUMERIC(9,6),
    lon           NUMERIC(9,6),
    value_m       NUMERIC(8,3),
    reading_at    TIMESTAMPTZ,
    polled_at     TIMESTAMPTZ DEFAULT NOW()
);

ALTER TABLE station_readings
ADD CONSTRAINT uq_station_reading
UNIQUE (station_ref, reading_at);
