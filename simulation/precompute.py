"""
One-time precompute step: CSV → feature-engineered Parquet for the simulation window.

Run:
    python -m simulation.precompute
    python -m simulation.precompute --force   # recompute even if Parquet exists
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

# Allow importing from models/ without installing as a package
sys.path.insert(0, str(Path(__file__).parent.parent / "models"))

import pandas as pd

from config import Config
from data_loader import WEATHER_COLS, _DTYPES
from feature_engineering import build_features
from thresholds import load_thresholds

WARMUP_START = "2020-10-01"   # 93-day buffer ensures valid 30-day rolling features
SIM_START    = "2021-01-01"   # First date included in the output Parquet
PARQUET_PATH = Path(__file__).parent / "data" / "simulation.parquet"


def run_precompute(config: Config, force: bool = False) -> Path:
    """
    Build and cache the feature-engineered Parquet for the simulation window.

    Loads the CSV from WARMUP_START onwards (only ~15% of the full dataset),
    engineers all lag/rolling features, adds the per-station threshold column,
    clips to SIM_START+, and writes a snappy-compressed Parquet.

    Parameters
    ----------
    config : Config
    force  : Recompute even if the Parquet already exists.

    Returns
    -------
    Path to the written Parquet file.
    """
    out = PARQUET_PATH
    if out.exists() and not force:
        print(f"Parquet already exists at {out} — skipping. Use --force to recompute.")
        return out

    # 1. Load only the rows we need — filter by date during CSV read using chunks
    #    so we never hold the full 11.6 M-row file in memory.
    print(f"Loading CSV from {WARMUP_START} onwards (chunked read) …")
    load_cols = ["station_ref", "date", "value_m", "quality"] + WEATHER_COLS
    chunks = []
    for chunk in pd.read_csv(
        config.csv_path,
        usecols=load_cols,
        dtype=_DTYPES,
        parse_dates=["date"],
        chunksize=500_000,
    ):
        chunk = chunk[
            (chunk["quality"] == "Good") & (chunk["date"] >= WARMUP_START)
        ].drop(columns=["quality"])
        if not chunk.empty:
            chunks.append(chunk)

    df = (
        pd.concat(chunks, ignore_index=True)
        .drop_duplicates(subset=["station_ref", "date"])
        .sort_values(["station_ref", "date"])
        .reset_index(drop=True)
    )
    print(f"  {len(df):,} rows loaded ({df['station_ref'].nunique():,} stations)")

    # 2. Feature engineering (lag/rolling; warmup rows provide history for 2021-01-01)
    df = build_features(df, config)

    # 3. Add per-station threshold for display in station popups
    thresholds_path = Path(config.artifacts_dir) / "thresholds.json"
    if thresholds_path.exists():
        thresholds = load_thresholds(str(thresholds_path))
        df["threshold"] = df["station_ref"].map(thresholds).astype("float32")
    else:
        print(f"  Warning: thresholds.json not found at {thresholds_path} — threshold column will be NaN")
        df["threshold"] = float("nan")

    # 4. Clip to simulation window (drop warmup rows that served as lookback history)
    df = df[df["date"] >= SIM_START].copy().reset_index(drop=True)

    # 5. Normalise station_ref to string (category dtype causes Parquet issues)
    df["station_ref"] = df["station_ref"].astype(str)

    # 6. Write Parquet
    out.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out, index=False, compression="snappy", engine="pyarrow")

    size_mb = out.stat().st_size / 1e6
    print(f"  Saved {len(df):,} rows × {df.shape[1]} cols → {out} ({size_mb:.0f} MB)")
    return out


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Precompute simulation Parquet")
    parser.add_argument("--force", action="store_true", help="Recompute even if Parquet exists")
    args = parser.parse_args()
    run_precompute(Config(), force=args.force)
