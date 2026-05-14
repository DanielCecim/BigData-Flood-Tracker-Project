"""
LightGBM binary classifier for next-day flood exceedance.

Uses tabular lag/rolling features engineered in feature_engineering.py.
Station identity is label-encoded and passed as a categorical feature.
Class imbalance (~10% positives at P90) is handled via scale_pos_weight.
"""

from __future__ import annotations

import pickle
from pathlib import Path

import lightgbm as lgb
import numpy as np
import pandas as pd
from sklearn.preprocessing import LabelEncoder

from config import Config

# Columns that are not predictive features
_META_COLS = {"station_ref", "date", "value_m", "is_flood", "threshold", "target"}


class LGBMFloodModel:
    def __init__(self, config: Config):
        self.config = config
        self.model: lgb.Booster | None = None
        self.label_encoder = LabelEncoder()
        self.feature_cols: list[str] = []

    # ------------------------------------------------------------------ #

    def _to_matrix(self, df: pd.DataFrame) -> tuple[np.ndarray, np.ndarray | None]:
        """Build feature matrix and optional label array from a dataframe."""
        # Encode station as integer category
        station_enc = self.label_encoder.transform(
            df["station_ref"].astype(str)
        ).astype(np.int32)

        feature_df = df.drop(
            columns=[c for c in _META_COLS if c in df.columns]
        ).copy()
        feature_df.insert(0, "station_enc", station_enc)

        if not self.feature_cols:
            self.feature_cols = list(feature_df.columns)

        X = feature_df[self.feature_cols].values.astype(np.float32)
        y = df["target"].values if "target" in df.columns else None
        return X, y

    # ------------------------------------------------------------------ #

    def fit(self, train_df: pd.DataFrame, val_df: pd.DataFrame) -> None:
        # Only fit encoder if not already done (caller may pre-fit on all splits)
        if not hasattr(self.label_encoder, "classes_"):
            all_stations = pd.concat(
                [train_df["station_ref"], val_df["station_ref"]]
            ).astype(str).unique()
            self.label_encoder.fit(all_stations)

        X_train, y_train = self._to_matrix(train_df)
        X_val, y_val = self._to_matrix(val_df)

        # Drop rows where target is NaN (last row per station has no next day)
        mask_tr = ~np.isnan(y_train)
        mask_va = ~np.isnan(y_val)

        pos_rate = float(y_train[mask_tr].mean())
        scale_pos_weight = (1 - pos_rate) / pos_rate
        print(
            f"  Positive rate = {pos_rate:.3f} "
            f"→ scale_pos_weight = {scale_pos_weight:.1f}"
        )

        dtrain = lgb.Dataset(
            X_train[mask_tr],
            label=y_train[mask_tr],
            feature_name=self.feature_cols,
            categorical_feature=["station_enc"],
            free_raw_data=False,
        )
        dval = lgb.Dataset(
            X_val[mask_va],
            label=y_val[mask_va],
            feature_name=self.feature_cols,
            categorical_feature=["station_enc"],
            free_raw_data=False,
            reference=dtrain,
        )

        params = {
            "objective": "binary",
            "metric": ["binary_logloss", "auc"],
            "learning_rate": self.config.lgbm_learning_rate,
            "num_leaves": self.config.lgbm_num_leaves,
            "max_depth": self.config.lgbm_max_depth,
            "min_child_samples": self.config.lgbm_min_child_samples,
            "subsample": self.config.lgbm_subsample,
            "colsample_bytree": self.config.lgbm_colsample_bytree,
            "scale_pos_weight": scale_pos_weight,
            "seed": self.config.random_seed,
            "verbosity": -1,
            "n_jobs": -1,
        }

        self.model = lgb.train(
            params,
            dtrain,
            num_boost_round=self.config.lgbm_n_estimators,
            valid_sets=[dval],
            callbacks=[
                lgb.early_stopping(
                    self.config.lgbm_early_stopping_rounds, verbose=True
                ),
                lgb.log_evaluation(self.config.lgbm_log_period),
            ],
        )

    # ------------------------------------------------------------------ #

    def predict_proba(self, df: pd.DataFrame) -> np.ndarray:
        X, _ = self._to_matrix(df)
        return self.model.predict(X)

    def feature_importance(self) -> pd.Series:
        imp = self.model.feature_importance(importance_type="gain")
        return pd.Series(imp, index=self.feature_cols).sort_values(ascending=False)

    # ------------------------------------------------------------------ #

    def save(self, path: str) -> None:
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "model": self.model,
            "label_encoder": self.label_encoder,
            "feature_cols": self.feature_cols,
            "config": self.config,
        }
        with open(path, "wb") as f:
            pickle.dump(payload, f)
        print(f"  LightGBM model saved → {path}")

    @classmethod
    def load(cls, path: str) -> "LGBMFloodModel":
        with open(path, "rb") as f:
            data = pickle.load(f)
        obj = cls(data["config"])
        obj.model = data["model"]
        obj.label_encoder = data["label_encoder"]
        obj.feature_cols = data["feature_cols"]
        return obj
