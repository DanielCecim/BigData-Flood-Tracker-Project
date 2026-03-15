from __future__ import annotations
import psycopg2


def insert_station_reading(cursor, reading: dict) -> bool:
    """
    Insert one station reading. Returns True if inserted, False if skipped.
    Skips silently when (station_ref, reading_at) already exists.
    """
    cursor.execute("""
        INSERT INTO station_readings
            (station_ref, station_label, lat, lon, value_m, reading_at)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (station_ref, reading_at) DO NOTHING
    """, (
        reading["ref"],
        reading["label"],
        reading.get("lat"),
        reading.get("lon"),
        reading["value"],
        reading["reading_at"],
    ))
    return cursor.rowcount == 1


def insert_station_readings_batch(cursor, readings: list[dict]) -> int:
    """
    Batch insert all station readings for one poll cycle.
    Skips rows where (station_ref, reading_at) already exists.
    Returns the number of rows actually inserted.
    """
    if not readings:
        return 0
    rows = [
        (r["ref"], r["label"], r.get("lat"), r.get("lon"),
         r["value"], r["reading_at"])
        for r in readings
    ]
    cursor.executemany("""
        INSERT INTO station_readings
            (station_ref, station_label, lat, lon, value_m, reading_at)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (station_ref, reading_at) DO NOTHING
    """, rows)
    return cursor.rowcount


def insert_flood_event(cursor, flood: dict, polled_at: str) -> int | None:
    """
    Insert a flood warning. Updates severity/message if changed.
    Returns row id if inserted or updated, None if identical (no-op).
    """
    cursor.execute("""
        INSERT INTO flood_events (
            flood_area_id, description, county, river_or_sea,
            severity_level, severity_label, is_tidal,
            lat, lon, message, time_raised, time_severity_changed, polled_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (flood_area_id, polled_at) DO UPDATE
            SET severity_level        = EXCLUDED.severity_level,
                severity_label        = EXCLUDED.severity_label,
                message               = EXCLUDED.message,
                time_severity_changed = EXCLUDED.time_severity_changed
        WHERE
            flood_events.severity_level           IS DISTINCT FROM EXCLUDED.severity_level
            OR flood_events.message               IS DISTINCT FROM EXCLUDED.message
            OR flood_events.time_severity_changed IS DISTINCT FROM EXCLUDED.time_severity_changed
        RETURNING id
    """, (
        flood.get("floodAreaID"), flood.get("description"),
        flood.get("county"), flood.get("river_or_sea"),
        flood.get("severity_level"), flood.get("severity_label"),
        flood.get("is_tidal", False), flood.get("lat"), flood.get("lon"),
        flood.get("message"), flood.get("time_raised"),
        flood.get("time_severity_changed"), polled_at,
    ))
    row = cursor.fetchone()
    return row[0] if row else None


def insert_weather_snapshot(
    cursor,
    flood_event_id: int,
    weather: dict,
    polled_at: str,
) -> bool:
    """
    Insert a weather snapshot linked to a flood event.
    Returns True if inserted, False if (flood_event_id, polled_at) already exists.

    Field mapping from Open-Meteo API 2:
        temperature_c      <- current_weather.temperature
        precipitation_mm   <- hourly.precipitation[0]       (current hour)
        wind_speed_kmh     <- current_weather.windspeed
        wind_direction_deg <- current_weather.winddirection
        weather_code       <- current_weather.weathercode   (WMO standard)
        humidity_pct       <- hourly.relativehumidity_2m[0] (current hour)
    """
    cursor.execute("""
        INSERT INTO weather_snapshots (
            flood_event_id, polled_at, temperature_c, precipitation_mm,
            wind_speed_kmh, wind_direction_deg, weather_code, humidity_pct
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (flood_event_id, polled_at) DO NOTHING
    """, (
        flood_event_id, polled_at,
        weather.get("temperature_c"), weather.get("precipitation_mm"),
        weather.get("wind_speed_kmh"), weather.get("wind_direction_deg"),
        weather.get("weather_code"), weather.get("humidity_pct"),
    ))
    return cursor.rowcount == 1
