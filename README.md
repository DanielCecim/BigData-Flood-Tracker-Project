# UK Flood Prediction Pipeline

End-to-end system for predicting river-level exceedance and surfacing early flood warnings across the Environment Agency gauging network in England and Wales. The project has two parts:

- **Part 1** — Data integration pipeline. Live polling and 25-year historical backfill of five public APIs into a normalised PostgreSQL schema in Supabase.
- **Part 2** — Machine learning, simulation and production architecture. LightGBM quantile regression predicting next-day water level, station-specific thresholds, a FastAPI simulation UI replaying 2023–2024, and a discussion of how this would be deployed in production.

Group 8 — Big Data Technology, IE University. Professor: Adolfo Nadal Serrano.

---

## Repository structure

```
.
├── README.md
├── requirements.txt
├── .env                       # not committed — see "Setup" below
│
├── src/                       # Part 1 — data ingestion library
│   ├── db.py                  # Supabase/PostgreSQL connection factory
│   ├── live/                  # Live layer (12-hourly poll cycle)
│   └── backfill/              # Historical backfill (one-off)
│
├── sql/                       # PostgreSQL schema (run once, in numeric order)
│   ├── 002_historical_river_levels.sql
│   ├── 003_historical_weather.sql
│   ├── 006_flood_events.sql
│   ├── 007_weather_snapshots.sql
│   └── 008_station_readings.sql
│
├── models/                    # Part 2 — ML pipeline
│   ├── config.py
│   ├── data_loader.py
│   ├── feature_engineering.py
│   ├── thresholds.py          # P90-of-annual-maxima per-station thresholds
│   ├── lightgbm_model.py      # binary classifier baseline (rejected)
│   ├── lstm_model.py          # sequence model (rejected — data-starved)
│   ├── quantile_model.py      # nine-quantile LightGBM regression (final)
│   ├── train.py               # training entry point
│   ├── evaluate.py
│   ├── modal_app.py           # Modal cloud training runner
│   └── artifacts/             # trained models (.pkl, not committed)
│
├── simulation/                # Part 2 — FastAPI replay UI
│   ├── app.py                 # FastAPI server
│   ├── engine.py              # inference engine (alert tier classification)
│   ├── precompute.py          # builds simulation.parquet
│   ├── static/                # HTML/JS front-end
│   └── data/                  # simulation.parquet (not committed)
│
├── scripts/                   # CLI entry points (run from project root)
│   ├── flood_monitor.py       # live poll + backfill orchestrator
│   ├── run_sql.py             # apply a SQL file
│   ├── export_ml_data.py      # dump joined historical data to CSV
│   ├── generate_report_visuals.py
│   └── compare_alerts.py
│
├── report/                    # Report deliverables and their figures
│   └── visuals/               # PNGs and metrics JSON
│
├── exports/                   # ml_dataset.csv (not committed — regenerate)
└── logs/                      # backfill progress logs + station catalogue cache
```

---

## Setup

**1. Python environment.** Python 3.12 recommended.

```
python -m venv .venv
.venv\Scripts\activate          # Windows
source .venv/bin/activate         # macOS/Linux
pip install -r requirements.txt
```

**2. Database credentials.** Create a `.env` in the project root:

```
SUPABASE_HOST=aws-0-eu-west-1.pooler.supabase.com
SUPABASE_PORT=6543
SUPABASE_DB=postgres
SUPABASE_USER=postgres.<project-ref>
SUPABASE_PASSWORD=<password>
```

Port `6543` is the **transaction pooler** — use it. The session pooler (`5432`) drops idle connections after ~10 minutes and silently breaks long backfills.

**3. Apply the schema.** Run each SQL file once, in numeric order:

```
python scripts/run_sql.py sql/002_historical_river_levels.sql
python scripts/run_sql.py sql/003_historical_weather.sql
python scripts/run_sql.py sql/006_flood_events.sql
python scripts/run_sql.py sql/007_weather_snapshots.sql
python scripts/run_sql.py sql/008_station_readings.sql
```

---

## How to run

All commands assume you are in the project root.

### Part 1 — Data pipeline

**Live poll (one cycle, ~30 s).** Polls EA Real-Time + Open-Meteo Current and writes to the three live tables atomically.

```
python scripts/flood_monitor.py --once
```

**Historical river backfill** (resume-safe, ~18–55 hours for the full network):

```
python scripts/flood_monitor.py --backfill-rivers --start-year 2000 --end-year 2024
```

**Historical weather backfill** (Open-Meteo Archive, rate-limited to 10 s between stations):

```
python scripts/flood_monitor.py --backfill-weather --start-date 2000-01-01
```

Both backfills append to `logs/*.log` and can be killed and resumed without losing progress.

### Part 2 — ML pipeline

**Export training data** (joins `historical_river_levels` and `historical_weather` to `exports/ml_dataset.csv`):

```
python scripts/export_ml_data.py
```

**Train models** (LightGBM quantile is the production model; LSTM is the rejected sequence baseline kept for the report):

```
python models/train.py --model quantile
python models/train.py --model lgbm        # binary classifier baseline
python models/train.py --model lstm        # sequence baseline (data-starved)
```

Trained artifacts land in `models/artifacts/`. They are gitignored — train locally or download from Modal volume.

### Part 2 — Simulation UI

**Build the simulation parquet** (one-time, from the trained model + joined features):

```
python -m simulation.precompute
```

**Start the FastAPI server** and open http://localhost:8000:

```
uvicorn simulation.app:app --port 8000
```

### Regenerating report visuals

Recomputes ROC, quantile coverage and alert-timeline PNGs into `report/visuals/`:

```
python scripts/generate_report_visuals.py
```

---

## What's not committed

Per `.gitignore`:

- `.env` — DB credentials.
- `models/artifacts/*.pkl` — trained models (~200 MB each).
- `simulation/data/*.parquet` — precomputed inference parquet (~176 MB).
- `exports/` — the joined ML dataset CSV (~773 MB).
- `logs/*.log` — backfill progress logs (the station catalogue JSON *is* kept).
- `__pycache__/`, `.venv/`, IDE folders.

Each of these is regenerable by following the steps above.
