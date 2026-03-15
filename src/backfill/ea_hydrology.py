"""
EA Hydrology Data Explorer API client — historical river level data.

API base URL: https://environment.data.gov.uk/hydrology
No authentication required. Open Government Licence.

Fetches:
  - Station catalogue: all stations with water level records + coordinates
  - Daily mean water levels per station per year (annual chunks required)

Only rows with quality == 'Good' are returned. Missing or failed requests
return empty lists so the caller can continue without crashing.
"""

from __future__ import annotations
import json
import time
import urllib.parse
import urllib.request

BASE_URL  = "https://environment.data.gov.uk/hydrology"
TIMEOUT   = 30
RETRIES   = 3
BACKOFF   = 2


def _get(url: str) -> dict | None:
    """
    Issue an HTTP GET and return the parsed JSON response body.
    Retries up to RETRIES times with BACKOFF seconds between attempts.
    Returns None on total failure — never raises.
    """
    for attempt in range(1, RETRIES + 1):
        try:
            req = urllib.request.Request(
                url,
                headers={"Accept": "application/json"}
            )
            with urllib.request.urlopen(req, timeout=TIMEOUT) as resp:
                return json.loads(resp.read().decode())
        except Exception as e:
            if attempt == RETRIES:
                print(f"[EA Hydrology] FAILED after {RETRIES} attempts: {url} — {e}")
                return None
            time.sleep(BACKOFF)
    return None


def fetch_station_catalogue() -> list[dict]:
    """
    Fetch all EA Hydrology stations that have water level records.

    Returns a list of dicts, each with:
        station_ref  — EA station reference code
        label        — human-readable station name
        lat          — latitude (WGS84), may be None
        lon          — longitude (WGS84), may be None
        catchment    — river catchment name, may be None

    Returns [] if the request fails.
    """
    stations = []
    offset   = 0
    limit    = 500

    while True:
        url = (
            f"{BASE_URL}/id/stations"
            f"?observedProperty=waterLevel"
            f"&_limit={limit}"
            f"&_offset={offset}"
        )
        data = _get(url)
        if not data or "items" not in data:
            break

        items = data["items"]
        if not items:
            break

        for item in items:
            # Extract the first daily water-level measure URL from the measures list.
            # Daily measures have parameter='level' and period=86400 (seconds = 1 day).
            measure_url = None
            for m in item.get("measures", []):
                if not isinstance(m, dict):
                    continue
                if m.get("parameter") == "level" and str(m.get("period", "")) == "86400":
                    measure_url = m.get("@id")
                    break

            stations.append({
                "station_ref":             item.get("stationReference"),
                "label":                   item.get("label"),
                "lat":                     item.get("lat"),
                "lon":                     item.get("long"),   # renamed from 'long' to avoid SQL reserved word
                "catchment":               item.get("catchmentName"),
                "daily_level_measure_url": measure_url,
            })

        if len(items) < limit:
            break  # last page

        offset += limit

    return stations


def fetch_daily_levels(station_ref: str, year: int, measure_url: str | None = None) -> list[dict]:
    """
    Fetch daily water level readings for one station for one calendar year.

    Uses the EA Hydrology /data/readings endpoint filtered by a specific
    measure URL. The measure URL is the @id of the daily level measure,
    extracted from the station catalogue (see fetch_station_catalogue).

    EA Hydrology stations use UUIDs internally — the station-level measures
    endpoint requires the UUID notation, not the stationReference code used
    by the real-time API. The measure URL obtained from the catalogue
    already contains the correct UUID.

    Args:
        station_ref:  EA station reference code — used for DB storage
        year:         calendar year to fetch (e.g. 2005)
        measure_url:  full @id URL of the daily level measure for this station.
                      If None, the station has no daily level data — returns [].

    Returns a list of dicts, each with:
        station_ref — passed through from argument
        date        — date string YYYY-MM-DD
        value_m     — daily water level in metres (float)
        quality     — quality flag from EA API
        source      — always 'EA'

    Only rows where quality == 'Good' are included.
    Returns [] if measure_url is None, the request fails, or no good data.
    """
    if not measure_url:
        return []

    start = f"{year}-01-01"
    end   = f"{year}-12-31"

    url = (
        f"{BASE_URL}/data/readings"
        f"?measure={urllib.parse.quote(measure_url, safe='')}"
        f"&mineq-date={start}"
        f"&maxeq-date={end}"
        f"&_limit=500"
    )

    data = _get(url)
    if not data or "items" not in data:
        return []

    rows = []
    for item in data.get("items", []):
        quality = item.get("quality", "")
        if quality != "Good":
            continue
        value = item.get("value")
        if value is None:
            continue
        rows.append({
            "station_ref": station_ref,
            "date":        item.get("date"),
            "value_m":     float(value),
            "quality":     quality,
            "source":      "EA",
        })

    return rows
