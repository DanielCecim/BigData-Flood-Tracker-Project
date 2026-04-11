"""
Open-Meteo Archive API client — historical weather data (ERA5 reanalysis).

API base URL: https://archive-api.open-meteo.com/v1/archive
No authentication required. No rate limits for non-commercial use.
Data source: ECMWF ERA5 reanalysis — coverage from 1940 to ~5 days before today.
Spatial resolution: ~31 km (ERA5 native grid).

Unlike the current-weather API which is called per flood event, this API is
called once per station for the entire 20-year date range. The response
contains the full daily time series for all requested variables.

Null values in the response arrays mean missing data — these are stored as
NULL in the database. The row is not skipped; only the null field is absent.
"""

from __future__ import annotations
import json
import time
import urllib.error
import urllib.request
from datetime import date

BASE_URL = "https://archive-api.open-meteo.com/v1/archive"
TIMEOUT  = 60   # longer timeout — 20-year responses are large
RETRIES  = 3
BACKOFF  = 3


def _get(url: str) -> dict:
    """HTTP GET with retry. On 429, fails immediately (caller sleeps between stations)."""
    for attempt in range(1, RETRIES + 1):
        try:
            req = urllib.request.Request(url, headers={"Accept": "application/json"})
            with urllib.request.urlopen(req, timeout=TIMEOUT) as resp:
                return json.loads(resp.read().decode())
        except urllib.error.HTTPError as e:
            if e.code == 429:
                # Do not retry — let the between-station sleep + restart gap clear the limit
                raise RuntimeError(f"[Open-Meteo Archive] 429 rate limited: {url}")
            if attempt == RETRIES:
                raise RuntimeError(f"[Open-Meteo Archive] FAILED: {url} — {e}")
            time.sleep(BACKOFF)
        except Exception as e:
            if attempt == RETRIES:
                raise RuntimeError(f"[Open-Meteo Archive] FAILED: {url} — {e}")
            time.sleep(BACKOFF)
    raise RuntimeError("unreachable")


def fetch_weather_history(
    station_ref: str,
    lat:         float,
    lon:         float,
    start_date:  str = "2000-01-01",
    end_date:    str | None = None,
) -> list[dict]:
    """
    Fetch the complete daily weather history for one station coordinate.

    Retrieves 8 daily variables from the ERA5 reanalysis archive. The full
    date range is fetched in a single request — no annual chunking needed.
    All arrays in the response are positionally aligned with the time array.

    Args:
        station_ref: EA station reference code — used for DB storage
        lat:         latitude of the station (WGS84)
        lon:         longitude of the station (WGS84)
        start_date:  first date to fetch, format YYYY-MM-DD (default 2000-01-01)
        end_date:    last date to fetch, format YYYY-MM-DD (default: today)

    Returns a list of dicts, one per day, each with:
        station_ref             — passed through from argument
        date                    — date string YYYY-MM-DD
        precipitation_sum       — total daily precipitation in mm (may be None)
        precipitation_hours     — hours of precipitation in the day (may be None)
        windspeed_max           — maximum wind speed at 10m in km/h (may be None)
        winddirection_dominant  — dominant wind direction in degrees (may be None)
        temperature_mean        — daily mean temperature in °C (may be None)
        temperature_min         — daily minimum temperature in °C (may be None)
        et0_evapotranspiration  — reference evapotranspiration in mm (may be None)
        shortwave_radiation_sum — total shortwave radiation in MJ/m² (may be None)

    Returns [] if the request fails.
    """
    if end_date is None:
        end_date = date.today().isoformat()

    url = (
        f"{BASE_URL}"
        f"?latitude={lat}"
        f"&longitude={lon}"
        f"&start_date={start_date}"
        f"&end_date={end_date}"
        f"&daily=precipitation_sum"
        f",precipitation_hours"
        f",windspeed_10m_max"
        f",winddirection_10m_dominant"
        f",temperature_2m_mean"
        f",temperature_2m_min"
        f",et0_fao_evapotranspiration"
        f",shortwave_radiation_sum"
        f"&timezone=Europe%2FLondon"
    )

    data = _get(url)  # raises RuntimeError on failure — caller's except will catch it
    if not data or "daily" not in data:
        return []

    daily = data["daily"]
    times = daily.get("time", [])

    if not times:
        return []

    # All arrays are positionally aligned with the time array
    precip_sum  = daily.get("precipitation_sum",          [None] * len(times))
    precip_hrs  = daily.get("precipitation_hours",        [None] * len(times))
    wind_max    = daily.get("windspeed_10m_max",          [None] * len(times))
    wind_dir    = daily.get("winddirection_10m_dominant", [None] * len(times))
    temp_mean   = daily.get("temperature_2m_mean",        [None] * len(times))
    temp_min    = daily.get("temperature_2m_min",         [None] * len(times))
    et0         = daily.get("et0_fao_evapotranspiration", [None] * len(times))
    radiation   = daily.get("shortwave_radiation_sum",    [None] * len(times))

    rows = []
    for i, t in enumerate(times):
        rows.append({
            "station_ref":            station_ref,
            "date":                   t,
            "precipitation_sum":      precip_sum[i],
            "precipitation_hours":    precip_hrs[i],
            "windspeed_max":          wind_max[i],
            "winddirection_dominant": wind_dir[i],
            "temperature_mean":       temp_mean[i],
            "temperature_min":        temp_min[i],
            "et0_evapotranspiration": et0[i],
            "shortwave_radiation_sum":radiation[i],
        })

    return rows
