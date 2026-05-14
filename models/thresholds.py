"""
Per-station flood thresholds.

IMPORTANT: thresholds are ALWAYS fitted on training data only and then
applied to validation / test splits. Fitting on the full dataset would
leak future distribution information into the target labels.
"""

import json
from pathlib import Path

import numpy as np
import pandas as pd


def fit_thresholds(train_df: pd.DataFrame, percentile: float = 90.0) -> dict:
    """Return {station_ref: threshold_m} computed from the training split."""
    thresholds = (
        train_df.groupby("station_ref", observed=True)["value_m"]
        .quantile(percentile / 100)
        .to_dict()
    )
    values = list(thresholds.values())
    print(
        f"  Thresholds fitted on {len(thresholds):,} stations "
        f"(median={np.median(values):.3f}m, "
        f"p10={np.percentile(values, 10):.3f}m, "
        f"p90={np.percentile(values, 90):.3f}m)"
    )
    return thresholds


def fit_thresholds_annual_max(
    train_df: pd.DataFrame,
    percentile: float = 90.0,
    min_years: int = 5,
    fallback_percentile: float = 99.0,
) -> dict:
    """
    Return {station_ref: threshold_m} based on the Pth percentile of
    per-station ANNUAL MAXIMA.

    P90 of annual maxima ≈ 1-in-10 year flood level — only reached during
    genuinely extreme yearly peaks. Avoids rivers that naturally run high
    in winter being permanently flagged.

    Stations with fewer than min_years of training data can't produce a
    reliable annual-max percentile, so they fall back to the fallback_percentile
    of their daily readings instead.

    Parameters
    ----------
    train_df           : Training split dataframe (inner-joined, no gaps).
    percentile         : Percentile of annual maxima (default 90 → 1-in-10 year level).
    min_years          : Minimum distinct years required to use annual-max method.
    fallback_percentile: Daily percentile for stations with insufficient history.
    """
    df = train_df.copy()
    df["year"] = pd.to_datetime(df["date"]).dt.year

    annual_max   = df.groupby(["station_ref", "year"], observed=True)["value_m"].max()
    n_years      = annual_max.groupby("station_ref", observed=True).size()

    stations_ok     = set(n_years[n_years >= min_years].index)
    stations_sparse = set(n_years.index) - stations_ok

    print(
        f"  Annual-max thresholds: {len(stations_ok):,} stations with ≥{min_years} years | "
        f"{len(stations_sparse):,} short-record → fallback P{fallback_percentile}"
    )

    thresholds_primary = (
        annual_max[annual_max.index.get_level_values("station_ref").isin(stations_ok)]
        .groupby("station_ref", observed=True)
        .quantile(percentile / 100)
        .to_dict()
    )

    thresholds_fallback = (
        df[df["station_ref"].isin(stations_sparse)]
        .groupby("station_ref", observed=True)["value_m"]
        .quantile(fallback_percentile / 100)
        .to_dict()
    )

    thresholds = {**thresholds_primary, **thresholds_fallback}

    # Sanity check: if a station's threshold is exceeded on more than 10% of
    # its own training days, the threshold is unreliable (datum shift, sensor
    # recalibration, or training period unrepresentative of current conditions).
    # Override those stations with a higher daily percentile.
    exceedance_rate = (
        df.groupby("station_ref", observed=True)
        .apply(lambda g: (g["value_m"] >= thresholds.get(g.name, float("inf"))).mean())
    )
    unreliable = set(exceedance_rate[exceedance_rate > 0.10].index)
    if unreliable:
        higher_fallback = (
            df[df["station_ref"].isin(unreliable)]
            .groupby("station_ref", observed=True)["value_m"]
            .quantile(0.999)
            .to_dict()
        )
        thresholds.update(higher_fallback)
        print(f"  Sanity check: {len(unreliable):,} stations had >10% exceedance rate → raised to P99.9")

    values = list(thresholds.values())
    print(
        f"  Thresholds fitted on {len(thresholds):,} stations total "
        f"(median={np.median(values):.3f}m, "
        f"p10={np.percentile(values, 10):.3f}m, "
        f"p90={np.percentile(values, 90):.3f}m)"
    )
    return thresholds


def label_exceedance(df: pd.DataFrame, thresholds: dict) -> pd.DataFrame:
    """Add 'is_flood' binary column: 1 when value_m >= station threshold."""
    df = df.copy()
    df["threshold"] = df["station_ref"].map(thresholds).astype("float32")
    df["is_flood"] = (df["value_m"] >= df["threshold"]).astype("int8")
    return df


def add_next_day_target(df: pd.DataFrame) -> pd.DataFrame:
    """
    Shift is_flood by -1 per station so 'target' = tomorrow's exceedance.
    The last row of each station will have NaN target (no following day).
    """
    df = df.copy()
    df["target"] = (
        df.groupby("station_ref", observed=True)["is_flood"]
        .shift(-1)
        .astype("float32")
    )
    return df


def add_next_day_value_target(df: pd.DataFrame) -> pd.DataFrame:
    """
    Shift value_m by -1 per station so 'target_value_m' = tomorrow's water level.
    The last row of each station will have NaN (no following day).
    """
    df = df.copy()
    df["target_value_m"] = (
        df.groupby("station_ref", observed=True)["value_m"]
        .shift(-1)
        .astype("float32")
    )
    return df


def save_thresholds(thresholds: dict, path: str) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    serialisable = {str(k): float(v) for k, v in thresholds.items()}
    with open(path, "w") as f:
        json.dump(serialisable, f, indent=2)
    print(f"  Thresholds saved → {path}")


def load_thresholds(path: str) -> dict:
    with open(path) as f:
        return json.load(f)
