CREATE TABLE IF NOT EXISTS flood_events (
    id                    SERIAL PRIMARY KEY,
    flood_area_id         TEXT,
    description           TEXT,
    county                TEXT,
    river_or_sea          TEXT,
    severity_level        INTEGER,
    severity_label        TEXT,
    is_tidal              BOOLEAN,
    lat                   NUMERIC(9,6),
    lon                   NUMERIC(9,6),
    message               TEXT,
    time_raised           TIMESTAMPTZ,
    time_severity_changed TIMESTAMPTZ,
    polled_at             TIMESTAMPTZ DEFAULT NOW()
);

ALTER TABLE flood_events
ADD CONSTRAINT uq_flood_event
UNIQUE (flood_area_id, polled_at);
