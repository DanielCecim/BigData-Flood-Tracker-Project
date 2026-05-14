"""
Compare alert distributions across model/threshold combinations.

Usage (from project root):
    python scripts/compare_alerts.py
"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "models"))

import pandas as pd
from config import Config
from quantile_model import LGBMQuantileModel
from thresholds import load_thresholds
from simulation.precompute import PARQUET_PATH

ALERT_LEVELS = ["warning", "alert", "watch", "normal"]


def _classify_alert(prob: float) -> str:
    if prob >= 0.35:   return "warning"
    elif prob >= 0.20: return "alert"
    elif prob >= 0.10: return "watch"
    else:              return "normal"


def compute_distribution(model_path: str, thresholds_path: str, label: str):
    print(f"\nLoading {label} …")
    model      = LGBMQuantileModel.load(model_path)
    thresholds = load_thresholds(thresholds_path)

    df = pd.read_parquet(PARQUET_PATH)
    df["station_ref"] = df["station_ref"].astype(str)

    # Filter to stations known by this model
    known = set(model.label_encoder.classes_)
    df = df[df["station_ref"].isin(known)].copy()
    print(f"  {len(df):,} station-days | {df['station_ref'].nunique():,} stations")

    probs = model.predict_exceedance_prob(df, thresholds)
    levels = [_classify_alert(float(p)) for p in probs]

    total = len(levels)
    counts = {lvl: levels.count(lvl) for lvl in ALERT_LEVELS}
    pct    = {lvl: round(counts[lvl] / total * 100, 2) for lvl in ALERT_LEVELS}

    print(f"\n  {'Level':<10} {'Count':>12}  {'%':>7}")
    print(f"  {'-'*32}")
    for lvl in ALERT_LEVELS:
        bar = "█" * int(pct[lvl] / 2)
        print(f"  {lvl:<10} {counts[lvl]:>12,}  {pct[lvl]:>6.2f}%  {bar}")

    return counts, pct


if __name__ == "__main__":
    artifacts = ROOT / "models" / "artifacts"

    models = [
        (str(artifacts / "quantile_model.pkl"),    str(artifacts / "thresholds.json"),    "Current model"),
    ]

    # Add any extra model files found (e.g. quantile_model_p90.pkl)
    for pkl in sorted(artifacts.glob("quantile_model_p*.pkl")):
        thr = artifacts / pkl.name.replace("quantile_model", "thresholds").replace(".pkl", ".json")
        if thr.exists():
            models.append((str(pkl), str(thr), pkl.stem))

    results = {}
    for mp, tp, label in models:
        if not Path(mp).exists():
            print(f"  Skipping {label} — file not found")
            continue
        counts, pct = compute_distribution(mp, tp, label)
        results[label] = pct

    if len(results) > 1:
        print("\n\n  Side-by-side comparison")
        print(f"  {'Level':<10}", end="")
        for label in results:
            print(f"  {label:>16}", end="")
        print()
        print(f"  {'-' * (10 + 18 * len(results))}")
        for lvl in ALERT_LEVELS:
            print(f"  {lvl:<10}", end="")
            for label in results:
                print(f"  {results[label][lvl]:>15.2f}%", end="")
            print()
