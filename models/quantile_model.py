"""
LightGBM quantile regression model for next-day river level prediction.

Trains one LGBMRegressor per quantile level; flood-exceedance probability
is derived by interpolating across quantile predictions against the
per-station P90 threshold.
"""

from __future__ import annotations

import pickle
from pathlib import Path
from typing import Dict, List

import lightgbm as lgb
import numpy as np
import pandas as pd
from sklearn.preprocessing import LabelEncoder

QUANTILES = [0.05, 0.10, 0.25, 0.50, 0.75, 0.90, 0.95, 0.97, 0.99]

_META_COLS = {
    "station_ref", "date", "value_m", "is_flood",
    "threshold", "target", "target_value_m",
}


def _interp_exceedance(q_vals: np.ndarray, qs: List[float], threshold: float) -> float:
    """
    Estimate P(true_value > threshold) via piecewise-linear interpolation
    across sorted quantile predictions.

    q_vals : predicted values at each quantile (sorted ascending)
    qs     : quantile levels matching q_vals
    """
    q_vals = np.sort(q_vals)  # enforce monotonicity (handles quantile crossing)

    if threshold <= q_vals[0]:
        return float(1.0 - qs[0])
    if threshold >= q_vals[-1]:
        return float(1.0 - qs[-1])

    for i in range(len(q_vals) - 1):
        if q_vals[i] <= threshold <= q_vals[i + 1]:
            dv = q_vals[i + 1] - q_vals[i]
            frac = (threshold - q_vals[i]) / (dv + 1e-9)
            alpha = qs[i] + frac * (qs[i + 1] - qs[i])
            return float(1.0 - alpha)

    return 0.0


class LGBMQuantileModel:
    """
    One LGBMRegressor per quantile level trained on next-day water level.

    Usage
    -----
    model = LGBMQuantileModel(config)
    model.fit(train_df, val_df)                          # train all quantiles
    q_preds = model.predict_quantiles(df)                # {q: array}
    probs   = model.predict_exceedance_prob(df, thresh)  # P(tomorrow > thresh)
    model.save("path/quantile_model.pkl")
    model = LGBMQuantileModel.load("path/quantile_model.pkl")
    """

    def __init__(self, config=None):
        self.config = config
        self.models: Dict[float, lgb.LGBMRegressor] = {}
        self.label_encoder = LabelEncoder()
        self._feature_cols: List[str] | None = None

    # ------------------------------------------------------------------ #
    #  Private helpers
    # ------------------------------------------------------------------ #

    def _feature_cols_from(self, df: pd.DataFrame) -> List[str]:
        return [c for c in df.columns if c not in _META_COLS]

    def _encode_station(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        refs = df["station_ref"].astype(str)
        # Map unknown stations to 0 rather than raising
        known = set(self.label_encoder.classes_)
        refs = refs.where(refs.isin(known), other=self.label_encoder.classes_[0])
        df["station_enc"] = self.label_encoder.transform(refs)
        return df

    def _to_X_y(self, df: pd.DataFrame):
        df = self._encode_station(df)
        if self._feature_cols is None:
            self._feature_cols = self._feature_cols_from(df)
        X = df[self._feature_cols].values.astype(np.float32)
        y = df["target_value_m"].values.astype(np.float32)
        return X, y

    def _to_X(self, df: pd.DataFrame) -> np.ndarray:
        df = self._encode_station(df)
        return df[self._feature_cols].values.astype(np.float32)

    # ------------------------------------------------------------------ #
    #  Public API
    # ------------------------------------------------------------------ #

    def fit(self, train_df: pd.DataFrame, val_df: pd.DataFrame) -> None:
        """Train one LGBMRegressor per quantile level."""
        all_refs = pd.concat(
            [train_df["station_ref"], val_df["station_ref"]]
        ).astype(str).unique()
        self.label_encoder.fit(all_refs)

        train_clean = train_df.dropna(subset=["target_value_m"])
        val_clean   = val_df.dropna(subset=["target_value_m"])

        X_train, y_train = self._to_X_y(train_clean)
        X_val,   y_val   = self._to_X_y(val_clean)

        for q in QUANTILES:
            print(f"  Quantile {q:.2f} …", flush=True)
            lgb_params = dict(
                objective="quantile",
                alpha=q,
                n_estimators=6000,
                learning_rate=0.05,
                num_leaves=127,
                min_child_samples=30,
                subsample=0.8,
                colsample_bytree=0.8,
                reg_alpha=0.05,
                reg_lambda=0.05,
                n_jobs=-1,
                verbose=-1,
            )
            m = lgb.LGBMRegressor(**lgb_params)
            m.fit(
                X_train, y_train,
                eval_set=[(X_val, y_val)],
                callbacks=[
                    lgb.early_stopping(stopping_rounds=100, verbose=False),
                    lgb.log_evaluation(period=500),
                ],
            )
            self.models[q] = m
            print(f"    best iter={m.best_iteration_}", flush=True)

    def predict_quantiles(self, df: pd.DataFrame) -> Dict[float, np.ndarray]:
        """Return {quantile: array of predicted next-day water levels}."""
        X = self._to_X(df)
        return {q: m.predict(X) for q, m in self.models.items()}

    def predict_exceedance_prob(
        self, df: pd.DataFrame, thresholds: dict
    ) -> np.ndarray:
        """
        Estimate P(next_day_value_m > station_threshold) for each row.
        Uses piecewise-linear interpolation across quantile predictions.
        """
        qs = sorted(self.models.keys())
        q_preds = self.predict_quantiles(df)
        q_matrix = np.stack([q_preds[q] for q in qs], axis=1)  # (N, Q)

        refs  = df["station_ref"].astype(str).values
        probs = np.empty(len(df), dtype=np.float32)

        for i in range(len(df)):
            thr = thresholds.get(refs[i])
            if thr is None or (isinstance(thr, float) and np.isnan(thr)):
                probs[i] = 0.0
            else:
                probs[i] = _interp_exceedance(q_matrix[i], qs, float(thr))

        return probs

    def save(self, path: str) -> None:
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        with open(path, "wb") as f:
            pickle.dump(self, f)
        print(f"  Quantile model saved → {path}")

    @classmethod
    def load(cls, path: str) -> "LGBMQuantileModel":
        with open(path, "rb") as f:
            return pickle.load(f)
