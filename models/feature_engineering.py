"""
Feature engineering for the flood prediction pipeline.

All lag and rolling operations use shift(≥1) so that features at time t
contain no information from time t onward — preventing data leakage.

Target is next-day exceedance, so features include today's raw values;
the model never sees tomorrow's measurements.
"""

import numpy as np
import pandas as pd

from config import Config
from data_loader import WEATHER_COLS

# Raw features used by the LSTM (no lags needed — sequence captures history)
LSTM_FEATURE_COLS = [
    "value_m",
    "precipitation_sum",
    "precipitation_hours",
    "windspeed_max",
    "temperature_mean",
    "temperature_min",
    "et0_evapotranspiration",
    "shortwave_radiation_sum",
    "wind_sin",
    "wind_cos",
    "sin_doy",
    "cos_doy",
]


# ------------------------------------------------------------------ #
#  Cyclical / calendar encodings
# ------------------------------------------------------------------ #

def _add_date_features(df: pd.DataFrame) -> pd.DataFrame:
    doy = df["date"].dt.dayofyear
    month = df["date"].dt.month
    df["sin_doy"] = np.sin(2 * np.pi * doy / 365).astype("float32")
    df["cos_doy"] = np.cos(2 * np.pi * doy / 365).astype("float32")
    df["sin_month"] = np.sin(2 * np.pi * month / 12).astype("float32")
    df["cos_month"] = np.cos(2 * np.pi * month / 12).astype("float32")
    return df


def _encode_wind_direction(df: pd.DataFrame) -> pd.DataFrame:
    """Replace degrees with sin/cos components (circular feature)."""
    rad = np.deg2rad(df["winddirection_dominant"])
    df["wind_sin"] = np.sin(rad).astype("float32")
    df["wind_cos"] = np.cos(rad).astype("float32")
    df = df.drop(columns=["winddirection_dominant"])
    return df


# ------------------------------------------------------------------ #
#  Lag helpers (all groupby-aware, no cross-station leakage)
# ------------------------------------------------------------------ #

def _lag(df: pd.DataFrame, col: str, lag: int) -> pd.Series:
    return df.groupby("station_ref", observed=True)[col].shift(lag).astype("float32")


def _rolling_mean(df: pd.DataFrame, col: str, window: int) -> pd.Series:
    """Rolling mean of col[t-window … t-1] (shift(1) guards against same-day leak)."""
    return (
        df.groupby("station_ref", observed=True)[col]
        .transform(lambda x: x.shift(1).rolling(window, min_periods=window // 2).mean())
        .astype("float32")
    )


def _rolling_std(df: pd.DataFrame, col: str, window: int) -> pd.Series:
    return (
        df.groupby("station_ref", observed=True)[col]
        .transform(lambda x: x.shift(1).rolling(window, min_periods=window // 2).std())
        .astype("float32")
    )


def _rolling_sum(df: pd.DataFrame, col: str, window: int) -> pd.Series:
    return (
        df.groupby("station_ref", observed=True)[col]
        .transform(lambda x: x.shift(1).rolling(window, min_periods=1).sum())
        .astype("float32")
    )


# ------------------------------------------------------------------ #
#  Main entry point
# ------------------------------------------------------------------ #

def build_features(df: pd.DataFrame, config: Config) -> pd.DataFrame:
    """
    Add all engineered features in-place (returns a copy).
    Safe to call on the full dataset before the temporal split because
    every rolling/lag operation only looks backward.
    """
    print("Engineering features …")
    df = df.copy()

    # 1. Cyclical date / wind encoding
    df = _add_date_features(df)
    df = _encode_wind_direction(df)

    # 2. River level lags
    for lag in config.river_lag_days:
        df[f"river_lag_{lag}d"] = _lag(df, "value_m", lag)

    # 3. River level rolling statistics
    for w in config.river_rolling_windows:
        df[f"river_rollmean_{w}d"] = _rolling_mean(df, "value_m", w)
        df[f"river_rollstd_{w}d"] = _rolling_std(df, "value_m", w)

    # 4. Weather lags (winddirection replaced by sin/cos lags)
    lag_weather = [c for c in WEATHER_COLS if c != "winddirection_dominant"]
    lag_weather += ["wind_sin", "wind_cos"]
    for col in lag_weather:
        for lag in config.weather_lag_days:
            df[f"{col}_lag_{lag}d"] = _lag(df, col, lag)

    # 5. Precipitation rolling sums (antecedent moisture)
    for w in config.precip_rolling_windows:
        df[f"precip_rollsum_{w}d"] = _rolling_sum(df, "precipitation_sum", w)

    print(f"  Feature matrix: {df.shape[0]:,} rows × {df.shape[1]} columns")
    return df
