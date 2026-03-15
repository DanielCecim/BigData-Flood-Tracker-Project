"""UK Flood Prediction Pipeline — CLI entry point."""
from __future__ import annotations
import argparse
import json
import urllib.request
import urllib.parse
from datetime import datetime, timezone

from src.db import get_connection
from src.live.ingest import (
    insert_flood_event,
    insert_station_readings_batch,
    insert_weather_snapshot,
)

EA_BASE = "https://environment.data.gov.uk/flood-monitoring"
WEATHER_BASE = "https://api.open-meteo.com/v1/forecast"


def _get(url: str) -> dict | None:
    try:
        req = urllib.request.Request(url, headers={"Accept": "application/json"})
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read().decode())
    except Exception as e:
        print(f"[WARN] HTTP error: {url} — {e}")
        return None


def fetch_flood_area_coords() -> dict[str, tuple[float, float]]:
    data = _get(f"{EA_BASE}/id/floodAreas?_limit=1500")
    coords: dict[str, tuple[float, float]] = {}
    if data and "items" in data:
        for item in data["items"]:
            notation = item.get("notation")
            lat = item.get("lat")
            lon = item.get("long")
            if notation and lat is not None and lon is not None:
                coords[notation] = (lat, lon)
    print(f"[Poll] Fetched coordinates for {len(coords)} flood areas")
    return coords


def fetch_active_warnings() -> list[dict]:
    data = _get(f"{EA_BASE}/id/floods?min-severity=3")
    if not data or "items" not in data:
        return []
    return data["items"]


def fetch_weather(lat: float, lon: float) -> dict:
    url = (
        f"{WEATHER_BASE}?latitude={lat}&longitude={lon}"
        f"&current_weather=true"
        f"&hourly=relativehumidity_2m,precipitation"
        f"&forecast_days=1&timezone=Europe%2FLondon"
    )
    data = _get(url)
    if not data:
        return {}
    cw = data.get("current_weather", {})
    hourly = data.get("hourly", {})
    return {
        "temperature_c":      cw.get("temperature"),
        "wind_speed_kmh":     cw.get("windspeed"),
        "wind_direction_deg": cw.get("winddirection"),
        "weather_code":       cw.get("weathercode"),
        "precipitation_mm":   (hourly.get("precipitation") or [None])[0],
        "humidity_pct":       (hourly.get("relativehumidity_2m") or [None])[0],
    }


def _extract_station_ref(item: dict) -> str | None:
    """
    Extract station reference from the reading item.
    The @id URL has the form: .../readings/{station_ref}-level-.../{datetime}
    """
    aid = item.get("@id", "")
    # Split on '/readings/' and take the part after it
    if "/readings/" in aid:
        segment = aid.split("/readings/")[-1].split("/")[0]
        # Station ref is everything before '-level-'
        if "-level-" in segment:
            return segment.split("-level-")[0]
    return None


def fetch_station_readings() -> list[dict]:
    data = _get(f"{EA_BASE}/data/readings?latest&parameter=level&_limit=500")
    if not data or "items" not in data:
        return []
    readings = []
    for item in data["items"]:
        value = item.get("value")
        if value is None:
            continue
        ref = _extract_station_ref(item)
        if not ref:
            continue
        readings.append({
            "ref":        ref,
            "label":      "",
            "lat":        None,
            "lon":        None,
            "value":      float(value),
            "reading_at": item.get("dateTime"),
        })
    return readings


def run_once() -> None:
    now = datetime.now(timezone.utc)
    # Truncate to the hour so repeated calls within the same hour are deduplicated
    polled_at = now.replace(minute=0, second=0, microsecond=0).isoformat()
    print(f"[Poll] Starting poll cycle at {polled_at}")

    coords = fetch_flood_area_coords()
    warnings = fetch_active_warnings()
    print(f"[Poll] {len(warnings)} active flood warnings")

    station_readings = fetch_station_readings()
    print(f"[Poll] {len(station_readings)} station readings")

    conn = get_connection()
    fe_inserted = fe_updated = fe_skipped = 0
    ws_inserted = ws_skipped = 0

    try:
        with conn.cursor() as cur:
            for flood_raw in warnings:
                area_id = flood_raw.get("floodAreaID")
                area = flood_raw.get("floodArea", {})
                lat, lon = coords.get(area_id, (None, None))

                flood = {
                    "floodAreaID":          area_id,
                    "description":          flood_raw.get("description"),
                    "county":               area.get("county"),
                    "river_or_sea":         area.get("riverOrSea"),
                    "severity_level":       flood_raw.get("severityLevel"),
                    "severity_label":       flood_raw.get("severity"),
                    "is_tidal":             flood_raw.get("isTidal", False),
                    "lat":                  lat,
                    "lon":                  lon,
                    "message":              flood_raw.get("message"),
                    "time_raised":          flood_raw.get("timeRaised"),
                    "time_severity_changed":flood_raw.get("timeSeverityChanged"),
                }

                event_id = insert_flood_event(cur, flood, polled_at)
                if event_id is None:
                    fe_skipped += 1
                else:
                    fe_inserted += 1

                # Fetch weather if we have coordinates and a valid event id
                if lat and lon and event_id is not None:
                    weather = fetch_weather(lat, lon)
                    ok = insert_weather_snapshot(cur, event_id, weather, polled_at)
                    if ok:
                        ws_inserted += 1
                    else:
                        ws_skipped += 1

            sr_inserted = insert_station_readings_batch(cur, station_readings)
            conn.commit()

    except Exception as e:
        conn.rollback()
        print(f"[ERROR] {e}")
        raise
    finally:
        conn.close()

    print(
        f"[Poll] Done. flood_events: {fe_inserted} inserted, {fe_skipped} skipped | "
        f"weather_snapshots: {ws_inserted} inserted, {ws_skipped} skipped | "
        f"station_readings: {sr_inserted} inserted"
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="UK Flood Prediction Pipeline")
    parser.add_argument("--once", action="store_true", help="Run one poll cycle and exit")
    parser.add_argument(
        "--backfill-rivers",
        action="store_true",
        help="Run historical river level backfill for all EA stations"
    )
    parser.add_argument(
        "--backfill-weather",
        action="store_true",
        help="Run historical weather backfill for all EA stations via Open-Meteo Archive"
    )
    parser.add_argument(
        "--start-year",
        type=int,
        default=2000,
        help="Start year for river backfill (default: 2000)"
    )
    parser.add_argument(
        "--end-year",
        type=int,
        default=2024,
        help="End year for river backfill (default: 2024)"
    )
    parser.add_argument(
        "--start-date",
        type=str,
        default="2000-01-01",
        help="Start date for weather backfill (default: 2000-01-01)"
    )
    args = parser.parse_args()

    if args.once:
        run_once()

    if args.backfill_rivers:
        from src.backfill.backfill_runner import run_river_backfill
        run_river_backfill(start_year=args.start_year, end_year=args.end_year)

    if args.backfill_weather:
        from src.backfill.backfill_runner import run_weather_backfill
        run_weather_backfill(start_date=args.start_date)


if __name__ == "__main__":
    main()
