import pandas as pd

from config import Config

WEATHER_COLS = [
    "precipitation_sum",
    "precipitation_hours",
    "windspeed_max",
    "winddirection_dominant",
    "temperature_mean",
    "temperature_min",
    "et0_evapotranspiration",
    "shortwave_radiation_sum",
]

_LOAD_COLS = ["station_ref", "date", "value_m", "quality"] + WEATHER_COLS

_DTYPES = {
    "station_ref": "category",
    "quality": "category",
    "value_m": "float32",
    **{c: "float32" for c in WEATHER_COLS},
}


def load_raw(config: Config) -> pd.DataFrame:
    """Load and minimally clean the ML dataset CSV."""
    print(f"Loading {config.csv_path} …")
    df = pd.read_csv(
        config.csv_path,
        usecols=_LOAD_COLS,
        dtype=_DTYPES,
        parse_dates=["date"],
    )
    df = (
        df
        .query("quality == 'Good'")
        .drop(columns=["quality"])
        .drop_duplicates(subset=["station_ref", "date"])
        .sort_values(["station_ref", "date"])
        .reset_index(drop=True)
    )
    print(
        f"  {len(df):,} rows | {df['station_ref'].nunique():,} stations "
        f"| {df['date'].min().date()} – {df['date'].max().date()}"
    )
    return df
