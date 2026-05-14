"""
Export joined river levels + weather data to a single CSV for ML training.

Joins historical_river_levels and historical_weather on (station_ref, date).
Streams results year by year to keep memory usage low.
Output: exports/ml_dataset.csv (under the project root)

Usage (from project root):
    python scripts/export_ml_data.py
"""

import csv
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from src.db import get_connection

OUTPUT_PATH = ROOT / "exports" / "ml_dataset.csv"
START_YEAR  = 2000
END_YEAR    = 2025

COLUMNS = [
    "station_ref", "date",
    "value_m", "quality",
    "precipitation_sum", "precipitation_hours",
    "windspeed_max", "winddirection_dominant",
    "temperature_mean", "temperature_min",
    "et0_evapotranspiration", "shortwave_radiation_sum",
]

QUERY = """
    SELECT
        r.station_ref,
        r.date,
        r.value_m,
        r.quality,
        w.precipitation_sum,
        w.precipitation_hours,
        w.windspeed_max,
        w.winddirection_dominant,
        w.temperature_mean,
        w.temperature_min,
        w.et0_evapotranspiration,
        w.shortwave_radiation_sum
    FROM historical_river_levels r
    INNER JOIN historical_weather w
        ON r.station_ref = w.station_ref AND r.date = w.date
    WHERE r.date >= %s AND r.date < %s
    ORDER BY r.station_ref, r.date
"""

OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)

total = 0
with open(OUTPUT_PATH, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(COLUMNS)

    for year in range(START_YEAR, END_YEAR + 1):
        start = f"{year}-01-01"
        end   = f"{year + 1}-01-01"

        conn = get_connection()
        try:
            with conn.cursor() as setup_cur:
                setup_cur.execute("SET statement_timeout = 0")
            conn.commit()
            with conn.cursor("export_cursor") as cur:
                cur.execute(QUERY, (start, end))
                year_count = 0
                while True:
                    batch = cur.fetchmany(10000)
                    if not batch:
                        break
                    writer.writerows(batch)
                    year_count += len(batch)
                total += year_count
                print(f"  {year}: {year_count:,} rows")
        finally:
            conn.close()

print(f"\nDone. {total:,} rows written to {OUTPUT_PATH}")
