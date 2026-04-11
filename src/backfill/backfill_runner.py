"""
Backfill runner — orchestrates the 20-year historical data collection.

Calls:
  - EA Hydrology API (API 3) for daily river levels per station per year
  - Open-Meteo Archive API (API 5) for daily weather per station

Resume safety:
  Both run_river_backfill() and run_weather_backfill() check BackfillProgress
  before every API call. A completed station-year is never re-fetched.
  Progress is written immediately after each successful insert, so a crash
  between station N and N+1 means only N+1 needs to be re-done on restart.

Usage:
  python flood_monitor.py --backfill-rivers --start 2000 --end 2024
  python flood_monitor.py --backfill-weather --start 2000 --end 2024
"""

from __future__ import annotations
import json
import os
import sys
import time

from src.db import get_connection
from src.backfill.progress import BackfillProgress
from src.backfill.ea_hydrology import fetch_station_catalogue, fetch_daily_levels
from src.backfill.open_meteo_archive import fetch_weather_history

RIVER_PROGRESS_LOG   = "logs/backfill_river_progress.log"
STATION_CATALOGUE_CACHE = "logs/ea_station_catalogue.json"
OPEN_METEO_SLEEP     = 240  # seconds between Open-Meteo requests to avoid 429s


def _load_station_catalogue() -> list[dict]:
    """Load EA station catalogue from disk cache if available, else fetch from API."""
    if os.path.exists(STATION_CATALOGUE_CACHE):
        with open(STATION_CATALOGUE_CACHE) as f:
            stations = json.load(f)
        print(f"[Backfill] Loaded {len(stations)} stations from cache")
        return stations
    stations = fetch_station_catalogue()
    with open(STATION_CATALOGUE_CACHE, "w") as f:
        json.dump(stations, f)
    print(f"[Backfill] Fetched and cached {len(stations)} stations")
    return stations
WEATHER_PROGRESS_LOG = "logs/backfill_weather_progress.log"


def _insert_river_levels(cursor, rows: list[dict]) -> int:
    """
    Batch insert river level rows using ON CONFLICT DO NOTHING.
    Returns the number of rows actually inserted.
    """
    if not rows:
        return 0
    cursor.executemany("""
        INSERT INTO historical_river_levels
            (station_ref, date, value_m, quality, source)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (station_ref, date) DO NOTHING
    """, [
        (r["station_ref"], r["date"], r["value_m"], r["quality"], r["source"])
        for r in rows
    ])
    return cursor.rowcount


def _insert_weather_rows(cursor, rows: list[dict]) -> int:
    """
    Batch insert weather rows using ON CONFLICT DO NOTHING.
    Returns the number of rows actually inserted.
    """
    if not rows:
        return 0
    cursor.executemany("""
        INSERT INTO historical_weather (
            station_ref, date,
            precipitation_sum, precipitation_hours,
            windspeed_max, winddirection_dominant,
            temperature_mean, temperature_min,
            et0_evapotranspiration, shortwave_radiation_sum
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (station_ref, date) DO NOTHING
    """, [
        (
            r["station_ref"], r["date"],
            r["precipitation_sum"], r["precipitation_hours"],
            r["windspeed_max"], r["winddirection_dominant"],
            r["temperature_mean"], r["temperature_min"],
            r["et0_evapotranspiration"], r["shortwave_radiation_sum"],
        )
        for r in rows
    ])
    return cursor.rowcount


def run_river_backfill(start_year: int = 2000, end_year: int = 2024) -> None:
    """
    Fetch and insert daily river level data for all EA stations, all years
    between start_year and end_year inclusive.

    Strategy:
      1. Fetch EA station catalogue
      2. For each station, for each year:
           - Skip if already in progress log (resume safety)
           - Fetch from EA Hydrology API
           - If EA returns 0 rows for this year, attempt NRFA gap-fill
           - Insert to historical_river_levels with ON CONFLICT DO NOTHING
           - Mark as done in progress log
      3. Print summary on completion
    """
    print(f"[Backfill] Starting river level backfill {start_year}–{end_year}")

    ea_stations = _load_station_catalogue()
    progress    = BackfillProgress(RIVER_PROGRESS_LOG)

    total    = len(ea_stations) * (end_year - start_year + 1)
    skipped  = 0
    inserted = 0
    failed   = 0

    for station in ea_stations:
        ref = station["station_ref"]
        if not ref:
            continue

        for year in range(start_year, end_year + 1):

            if progress.is_done(ref, year):
                skipped += 1
                continue

            try:
                measure_url = station.get("daily_level_measure_url")
                rows = fetch_daily_levels(ref, year, measure_url)

                # Fresh connection per insert — avoids idle connection timeouts
                conn = get_connection()
                try:
                    with conn.cursor() as cur:
                        n = _insert_river_levels(cur, rows)
                        conn.commit()
                        inserted += n
                finally:
                    conn.close()

                progress.mark_done(ref, year)
                if rows:
                    print(f"[OK]   {ref} {year} — {len(rows)} rows ({n} inserted)")
                else:
                    print(f"[SKIP] {ref} {year} — no data from EA or NRFA")

            except Exception as e:
                failed += 1
                print(f"[FAIL] {ref} {year} — {e}", file=sys.stderr)
                # Do NOT mark as done — will retry on next run
    print(
        f"\n[Backfill] River levels complete."
        f" Inserted: {inserted} | Skipped (done): {skipped}"
        f" | Failed: {failed} | Total station-years: {total}"
    )


def run_weather_backfill(start_date: str = "2000-01-01", end_date: str | None = None) -> None:
    """
    Fetch and insert 20+ years of daily weather for all EA stations using
    the Open-Meteo Archive API.

    Strategy:
      1. Fetch EA station catalogue (need coordinates)
      2. For each station:
           - Extract the year from start_date to use as progress key
           - Skip if already in progress log (resume safety)
           - Fetch full date range from Open-Meteo archive in one request
           - Insert all rows to historical_weather with ON CONFLICT DO NOTHING
           - Mark station as done using start_date year as the key
      3. Print summary on completion

    Note: Open-Meteo Archive returns the full date range in one call,
    so progress is tracked per station (not per station-year) for weather.
    The year in the progress key is taken from the start_date.
    """
    print(f"[Backfill] Starting weather backfill from {start_date}")

    ea_stations = _load_station_catalogue()
    progress    = BackfillProgress(WEATHER_PROGRESS_LOG)

    # Use a sentinel year (0) to represent the full-range weather fetch per station
    WEATHER_SENTINEL_YEAR = 0

    inserted = 0
    skipped  = 0
    failed   = 0

    for station in ea_stations:
        ref = station["station_ref"]
        lat = station["lat"]
        lon = station["lon"]

        if not ref or lat is None or lon is None:
            continue  # skip stations without coordinates

        if progress.is_done(ref, WEATHER_SENTINEL_YEAR):
            skipped += 1
            continue

        try:
            rows = fetch_weather_history(
                station_ref=ref,
                lat=float(lat),
                lon=float(lon),
                start_date=start_date,
                end_date=end_date,
            )

            # Fresh connection per insert — avoids idle connection timeouts
            conn = get_connection()
            try:
                with conn.cursor() as cur:
                    n = _insert_weather_rows(cur, rows)
                    conn.commit()
                    inserted += n
            finally:
                conn.close()

            progress.mark_done(ref, WEATHER_SENTINEL_YEAR)
            print(f"[OK] {ref} — {len(rows)} days fetched, {n} inserted")
            time.sleep(OPEN_METEO_SLEEP)  # respect Open-Meteo rate limit

        except Exception as e:
            failed += 1
            print(f"[FAIL] {ref} — {e}", file=sys.stderr)
    print(
        f"\n[Backfill] Weather complete."
        f" Inserted: {inserted} | Skipped (done): {skipped} | Failed: {failed}"
    )
