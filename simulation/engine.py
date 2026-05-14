"""
SimulationEngine — loaded once at server startup.

Holds the full simulation dataset and model in RAM and exposes two methods:
  get_station_metadata()  → list of all stations with lat/lon/threshold
  get_day_predictions(date_str) → list of per-station predictions for one day
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import List

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent / "models"))

from config import Config
from lightgbm_model import LGBMFloodModel
from quantile_model import LGBMQuantileModel
from thresholds import load_thresholds

from simulation.precompute import PARQUET_PATH, run_precompute

CATALOGUE_PATH = Path(__file__).parent.parent / "logs" / "ea_station_catalogue.json"


def _classify_alert(prob: float) -> str:
    if prob >= 0.90:   return "warning"
    elif prob >= 0.70: return "alert"
    elif prob >= 0.50: return "watch"
    else:              return "normal"


ALERT_LEVELS = ["normal", "watch", "alert", "warning"]


class SimulationEngine:
    def __init__(self) -> None:
        self.dates: List[str] = []
        self.stations: dict[str, dict] = {}
        self.stats: dict = {}
        self._date_index: dict[str, pd.DataFrame] = {}
        self._model: LGBMFloodModel | None = None
        self._quantile_model: LGBMQuantileModel | None = None
        self._thresholds: dict = {}
        self._use_quantile: bool = False

    # ------------------------------------------------------------------ #

    def load(self, config: Config) -> None:
        """
        Load all simulation assets into RAM. Blocking — call from a thread executor
        to avoid stalling the asyncio event loop.
        """
        # 1. Ensure Parquet exists (auto-precompute on first run)
        if not PARQUET_PATH.exists():
            print("Parquet not found — running precompute (this takes ~5–10 min) …")
            run_precompute(config)

        # 2. Load model — prefer quantile model if available
        quantile_path = Path(config.artifacts_dir) / "quantile_model.pkl"
        lgbm_path     = Path(config.artifacts_dir) / "lgbm_model.pkl"

        if quantile_path.exists():
            print("Loading quantile model …")
            self._quantile_model = LGBMQuantileModel.load(str(quantile_path))
            self._use_quantile   = True
            known_stations = set(self._quantile_model.label_encoder.classes_)
        elif lgbm_path.exists():
            print("Loading LightGBM model …")
            self._model    = LGBMFloodModel.load(str(lgbm_path))
            known_stations = set(self._model.label_encoder.classes_)
        else:
            raise FileNotFoundError(
                f"No model found in {config.artifacts_dir}\n"
                "Train one first:\n"
                "  modal run models/modal_app.py --model quantile"
            )

        # 3. Load thresholds
        thresholds_path = Path(config.artifacts_dir) / "thresholds.json"
        thresholds = load_thresholds(str(thresholds_path)) if thresholds_path.exists() else {}
        self._thresholds = thresholds

        # 4. Load station catalogue → {station_ref: {label, lat, lon}}
        print("Loading station catalogue …")
        with open(CATALOGUE_PATH) as f:
            catalogue_raw = json.load(f)
        catalogue: dict[str, dict] = {
            s["station_ref"]: {"label": s["label"], "lat": s["lat"], "lon": s["lon"]}
            for s in catalogue_raw
            if s.get("lat") is not None and s.get("lon") is not None
        }

        # 5. Load Parquet
        print("Loading simulation Parquet …")
        df = pd.read_parquet(PARQUET_PATH)
        df["date"] = pd.to_datetime(df["date"])

        # 6. Three-way intersection: Parquet ∩ catalogue ∩ model encoder
        parquet_stations = set(df["station_ref"].unique())
        valid = parquet_stations & set(catalogue.keys()) & known_stations
        print(
            f"  Parquet: {len(parquet_stations):,} | Catalogue: {len(catalogue):,} | "
            f"Model encoder: {len(known_stations):,} → Valid: {len(valid):,} stations"
        )
        df = df[df["station_ref"].isin(valid)].copy()

        # 7. Build station metadata dict (served to frontend for map init)
        for ref in valid:
            cat = catalogue[ref]
            self.stations[ref] = {
                "station_ref": ref,
                "label": cat["label"],
                "lat": float(cat["lat"]),
                "lon": float(cat["lon"]),
                "threshold": float(thresholds[ref]) if ref in thresholds else None,
            }

        # 8. Build date → DataFrame index for O(1) per-day lookup
        print("Indexing by date …")
        self._date_index = {
            str(date.date()): group.reset_index(drop=True)
            for date, group in df.groupby("date")
        }
        self.dates = sorted(self._date_index.keys())
        print(
            f"Simulation ready: {len(self.dates)} dates "
            f"({self.dates[0]} → {self.dates[-1]}), "
            f"{len(self.stations):,} stations"
        )

        # 9. Pre-compute full-period statistics (batch predict once)
        print("Computing statistics …")
        self.stats = self._compute_stats(df, catalogue)

    # ------------------------------------------------------------------ #

    def _predict_proba_batch(self, df: pd.DataFrame) -> np.ndarray:
        if self._use_quantile:
            return self._quantile_model.predict_exceedance_prob(df, self._thresholds)
        return np.array(self._model.predict_proba(df))

    def _compute_stats(self, df: pd.DataFrame, catalogue: dict) -> dict:
        """Batch-predict the full dataset and return aggregated statistics."""
        probs = self._predict_proba_batch(df)
        p = np.array(probs)
        print(
            f"  Prob distribution — "
            f"p50={np.percentile(p,50):.3f}  p90={np.percentile(p,90):.3f}  "
            f"p99={np.percentile(p,99):.3f}  max={p.max():.3f}"
        )

        df = df.copy()
        df["alert_level"] = [_classify_alert(float(v)) for v in probs]
        df["year"] = df["date"].dt.year

        total = len(df)
        counts = df["alert_level"].value_counts().to_dict()
        counts = {lvl: int(counts.get(lvl, 0)) for lvl in ALERT_LEVELS}
        pct    = {lvl: round(counts[lvl] / total * 100, 1) for lvl in ALERT_LEVELS}

        # By-year breakdown (only elevated levels for compactness)
        by_year = []
        for year, grp in df.groupby("year"):
            yc = grp["alert_level"].value_counts().to_dict()
            by_year.append({
                "year":    int(year),
                "watch":   int(yc.get("watch",   0)),
                "alert":   int(yc.get("alert",   0)),
                "warning": int(yc.get("warning", 0)),
            })

        # Top 15 stations by warning-day count
        warning_df = df[df["alert_level"] == "warning"]
        top_raw = (
            warning_df.groupby("station_ref")
            .size()
            .sort_values(ascending=False)
            .head(15)
        )
        top_stations = [
            {
                "station_ref":  ref,
                "label": catalogue.get(ref, {}).get("label", ref),
                "warning_days": int(cnt),
            }
            for ref, cnt in top_raw.items()
        ]

        return {
            "total_station_days": total,
            "counts": counts,
            "pct":    pct,
            "by_year": by_year,
            "top_stations": top_stations,
        }

    def get_stats(self) -> dict:
        return self.stats

    # ------------------------------------------------------------------ #

    def get_station_metadata(self) -> List[dict]:
        """All station dicts with lat/lon/label/threshold for map initialisation."""
        return list(self.stations.values())

    # ------------------------------------------------------------------ #

    def get_day_predictions(self, date_str: str) -> List[dict]:
        """
        Run the model on all stations for a given date.

        Returns a list of dicts:
            {station_ref, probability, alert_level, value_m, threshold,
             q10, q50, q90}   ← q10/q50/q90 present only for quantile model
        """
        day_df = self._date_index.get(date_str)
        if day_df is None or day_df.empty:
            return []

        if self._use_quantile:
            q_preds = self._quantile_model.predict_quantiles(day_df)
            probs   = self._quantile_model.predict_exceedance_prob(day_df, self._thresholds)
        else:
            probs   = self._model.predict_proba(day_df)
            q_preds = None

        results: List[dict] = []
        for i, row in enumerate(day_df.itertuples(index=False)):
            prob      = float(probs[i])
            value_m   = getattr(row, "value_m",   None)
            threshold = getattr(row, "threshold", None)

            entry = {
                "station_ref": str(row.station_ref),
                "probability": round(prob, 4),
                "alert_level": _classify_alert(prob),
                "value_m":    round(float(value_m),   3) if value_m   is not None and value_m   == value_m   else None,
                "threshold":  round(float(threshold), 3) if threshold is not None and threshold == threshold else None,
            }

            if q_preds is not None:
                entry["q10"] = round(float(q_preds[0.10][i]), 3)
                entry["q50"] = round(float(q_preds[0.50][i]), 3)
                entry["q90"] = round(float(q_preds[0.90][i]), 3)

            results.append(entry)
        return results
