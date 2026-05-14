"""
Training entry point for the UK flood prediction pipeline.

Temporal split (no shuffling, no row-level leakage):
  Train : date <= TRAIN_END_DATE        (2000 – 2020)
  Val   : TRAIN_END_DATE < date <= VAL_END_DATE  (2021 – 2022)
  Test  : date > VAL_END_DATE                    (2023 – 2024)

Threshold leakage prevention:
  Per-station 90th-percentile thresholds are fitted on the TRAIN split only,
  then applied identically to val and test.

Usage:
    python models/train.py --model lgbm
    python models/train.py --model lstm
    python models/train.py --model both
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

# Allow running as `python models/train.py` from the repo root
sys.path.insert(0, str(Path(__file__).parent))

import numpy as np
import pandas as pd

from config import Config
from data_loader import load_raw
from evaluate import evaluate, evaluate_quantile
from feature_engineering import build_features
from lightgbm_model import LGBMFloodModel
from lstm_model import LSTMTrainer
from quantile_model import LGBMQuantileModel
from thresholds import (
    add_next_day_target,
    add_next_day_value_target,
    fit_thresholds,
    fit_thresholds_annual_max,
    label_exceedance,
    save_thresholds,
)


# ------------------------------------------------------------------ #
#  Shared data preparation
# ------------------------------------------------------------------ #

def prepare_data(config: Config):
    """
    Returns (train_df, val_df, test_df) ready for model training.

    Order of operations — strictly avoids any form of leakage:
      1. Load raw data
      2. Engineer features (lag/rolling lookback only)
      3. Temporal split
      4. Fit thresholds on TRAIN only
      5. Label exceedance + add next-day target
      6. Drop rows without a valid target
    """
    df = load_raw(config)
    df = build_features(df, config)

    train = df[df["date"] <= config.train_end_date].copy()
    val = df[
        (df["date"] > config.train_end_date) & (df["date"] <= config.val_end_date)
    ].copy()
    test = df[df["date"] > config.val_end_date].copy()

    print(
        f"Split sizes — train: {len(train):,} | val: {len(val):,} | test: {len(test):,}"
    )

    # Thresholds from training data only
    print("Fitting thresholds …")
    thresholds = fit_thresholds(train, config.flood_percentile)
    save_thresholds(
        thresholds, str(Path(config.artifacts_dir) / "thresholds.json")
    )

    def _label(split):
        return add_next_day_target(
            label_exceedance(split, thresholds)
        ).dropna(subset=["target"])

    train = _label(train)
    val = _label(val)
    test = _label(test)

    return train, val, test


# ------------------------------------------------------------------ #
#  LightGBM
# ------------------------------------------------------------------ #

def train_lgbm(config: Config) -> dict:
    print("\n" + "=" * 60)
    print("  LightGBM Training")
    print("=" * 60)

    train, val, test = prepare_data(config)

    model = LGBMFloodModel(config)
    # Pre-fit encoder on ALL stations so test-period-only stations don't cause KeyError
    all_stations = pd.concat(
        [train["station_ref"], val["station_ref"], test["station_ref"]]
    ).astype(str).unique()
    model.label_encoder.fit(all_stations)

    model.fit(train, val)
    model.save(str(Path(config.artifacts_dir) / "lgbm_model.pkl"))

    # Feature importance
    imp = model.feature_importance().head(20)
    print("\nTop-20 features by gain:")
    print(imp.to_string())

    # Test evaluation
    y_prob = model.predict_proba(test)
    y_true = test["target"].values
    mask = ~np.isnan(y_prob) & ~np.isnan(y_true)

    return evaluate(
        y_true[mask],
        y_prob[mask],
        model_name="lgbm",
        save_dir=str(Path(config.artifacts_dir) / "plots"),
    )


# ------------------------------------------------------------------ #
#  LSTM
# ------------------------------------------------------------------ #

def train_lstm(config: Config) -> dict:
    print("\n" + "=" * 60)
    print("  LSTM Training")
    print("=" * 60)

    train, val, test = prepare_data(config)

    trainer = LSTMTrainer(config)
    trainer.fit(train, val)
    trainer.save(str(Path(config.artifacts_dir) / "lstm_model.pkl"))

    # Test evaluation — predict_proba returns (probs, valid_iloc_indices)
    test_reset = test.reset_index(drop=True)
    y_prob, valid_idx = trainer.predict_proba(test_reset)

    if len(y_prob) == 0:
        print("  No valid sequences in test set — skipping evaluation.")
        return {}

    y_true = test_reset.iloc[valid_idx]["target"].values

    return evaluate(
        y_true,
        y_prob,
        model_name="lstm",
        save_dir=str(Path(config.artifacts_dir) / "plots"),
    )


# ------------------------------------------------------------------ #
#  Quantile regression
# ------------------------------------------------------------------ #

def prepare_data_quantile(config: Config):
    """
    Like prepare_data() but target = next-day water level (regression).
    Both target_value_m AND binary target are added so the same splits
    can be reused for evaluation comparisons.
    """
    df = load_raw(config)
    df = build_features(df, config)

    train = df[df["date"] <= config.train_end_date].copy()
    val   = df[
        (df["date"] > config.train_end_date) & (df["date"] <= config.val_end_date)
    ].copy()
    test  = df[df["date"] > config.val_end_date].copy()

    print(f"Split sizes — train: {len(train):,} | val: {len(val):,} | test: {len(test):,}")

    print("Fitting thresholds (annual-max method) …")
    thresholds = fit_thresholds_annual_max(train, config.flood_percentile)
    save_thresholds(thresholds, str(Path(config.artifacts_dir) / "thresholds.json"))

    def _label(split):
        split = label_exceedance(split, thresholds)
        split = add_next_day_target(split)
        split = add_next_day_value_target(split)
        return split.dropna(subset=["target_value_m"])

    return _label(train), _label(val), _label(test), thresholds


def train_quantile(config: Config) -> dict:
    print("\n" + "=" * 60)
    print("  Quantile Regression Training")
    print("=" * 60)

    train, val, test, thresholds = prepare_data_quantile(config)

    model = LGBMQuantileModel(config)
    # Pre-fit encoder on all stations across splits
    all_stations = pd.concat(
        [train["station_ref"], val["station_ref"], test["station_ref"]]
    ).astype(str).unique()
    model.label_encoder.fit(all_stations)

    model.fit(train, val)
    model.save(str(Path(config.artifacts_dir) / "quantile_model.pkl"))

    # Evaluate on test set
    from quantile_model import QUANTILES
    q_preds = model.predict_quantiles(test)
    probs   = model.predict_exceedance_prob(test, thresholds)
    y_true  = test["target_value_m"].values

    return evaluate_quantile(
        y_true=y_true,
        q_preds=q_preds,
        quantiles=QUANTILES,
        exceedance_probs=probs,
        exceedance_true=(test["target"].fillna(0).values).astype(int),
        model_name="quantile",
        save_dir=str(Path(config.artifacts_dir) / "plots"),
    )


# ------------------------------------------------------------------ #
#  CLI
# ------------------------------------------------------------------ #

def main():
    parser = argparse.ArgumentParser(
        description="Train flood prediction models"
    )
    parser.add_argument(
        "--model",
        choices=["lgbm", "lstm", "quantile", "both"],
        default="lgbm",
        help="Which model to train (default: lgbm)",
    )
    parser.add_argument(
        "--percentile",
        type=float,
        default=90.0,
        help="Flood threshold percentile (default: 90)",
    )
    args = parser.parse_args()

    config = Config(flood_percentile=args.percentile)
    Path(config.artifacts_dir).mkdir(parents=True, exist_ok=True)

    if args.model in ("lgbm", "both"):
        metrics = train_lgbm(config)
        print("\nLightGBM metrics:", metrics)

    if args.model in ("lstm", "both"):
        metrics = train_lstm(config)
        print("\nLSTM metrics:", metrics)

    if args.model == "quantile":
        metrics = train_quantile(config)
        print("\nQuantile metrics:", metrics)


if __name__ == "__main__":
    main()
