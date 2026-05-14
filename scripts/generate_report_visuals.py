"""
Generate visuals for Part 2 of the Big Data report.

Outputs (saved to report/visuals/):
  - quantile_coverage.png    : Predicted q10/q50/q90 band vs actual on test set
  - roc_curve_test.png       : Flood-exceedance ROC on 2023-2024 test period
  - predicted_vs_actual.png  : Density scatter q50 vs actual next-day level
  - alert_timeline.png       : Daily count of stations in each alert tier (test period)
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "models"))

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from sklearn.metrics import roc_auc_score, roc_curve

from config import Config
from quantile_model import LGBMQuantileModel, QUANTILES
from thresholds import load_thresholds

OUT_DIR = ROOT / "report" / "visuals"
OUT_DIR.mkdir(parents=True, exist_ok=True)

CONFIG          = Config()
SIM_PARQUET     = ROOT / "simulation" / "data" / "simulation.parquet"
QUANTILE_MODEL  = ROOT / "models" / "artifacts" / "quantile_model.pkl"
THRESHOLDS_PATH = ROOT / "models" / "artifacts" / "thresholds.json"

TEST_START = "2023-01-01"
TEST_END   = "2024-12-31"


def _classify_alert(p: float) -> str:
    if p >= 0.90: return "Warning"
    if p >= 0.70: return "Alert"
    if p >= 0.50: return "Watch"
    return "Normal"


def main():
    print("Loading simulation parquet ...")
    df = pd.read_parquet(SIM_PARQUET)
    df["date"] = pd.to_datetime(df["date"])
    df = df[(df["date"] >= TEST_START) & (df["date"] <= TEST_END)].copy()
    print(f"  Test rows: {len(df):,}")

    # Build next-day actuals per station
    df = df.sort_values(["station_ref", "date"]).reset_index(drop=True)
    df["target_value_m"] = df.groupby("station_ref")["value_m"].shift(-1).astype("float32")
    df = df.dropna(subset=["target_value_m"]).reset_index(drop=True)

    print("Loading quantile model ...")
    model = LGBMQuantileModel.load(str(QUANTILE_MODEL))
    thresholds = load_thresholds(str(THRESHOLDS_PATH))

    # Strip non-feature columns the parquet has that the model wasn't trained on
    df_for_model = df.copy()

    print("Predicting quantiles (this takes ~1 minute) ...")
    q_preds = model.predict_quantiles(df_for_model)
    probs   = model.predict_exceedance_prob(df_for_model, thresholds)

    y_true = df["target_value_m"].values
    q10 = q_preds[0.10]
    q50 = q_preds[0.50]
    q90 = q_preds[0.90]

    # Binary flood label = next-day value >= station threshold
    thr_arr = df["station_ref"].map(thresholds).astype(float).values
    y_flood = (y_true >= thr_arr).astype(int)

    print(f"  flood positive rate (test): {y_flood.mean():.4%}")

    # ============================================================== ROC
    print("Plot 1: ROC curve")
    fpr, tpr, _ = roc_curve(y_flood, probs)
    auc = roc_auc_score(y_flood, probs)
    fig, ax = plt.subplots(figsize=(6.5, 5.5))
    ax.plot(fpr, tpr, lw=2.0, color="#1a6eb5", label=f"Quantile model  AUC = {auc:.3f}")
    ax.plot([0, 1], [0, 1], "k--", lw=0.8, label="Random")
    ax.set_xlabel("False positive rate", fontsize=10)
    ax.set_ylabel("True positive rate", fontsize=10)
    ax.set_title("Flood exceedance ROC — held-out test period 2023-2024",
                 fontsize=11, color="#1a3a5c", weight="bold")
    ax.legend(loc="lower right")
    ax.grid(alpha=0.25)
    fig.tight_layout()
    fig.savefig(OUT_DIR / "roc_curve_test.png", dpi=140)
    plt.close(fig)

    # ====================================================== Coverage plot
    # Restrict to typical river stations (median forecast in 0-5m range) for readability
    print("Plot 2: Quantile interval coverage")
    coverage = float(((y_true >= q10) & (y_true <= q90)).mean())

    typical_mask = (q50 >= -1) & (q50 <= 5)
    idx_pool = np.where(typical_mask)[0]
    if len(idx_pool) > 4000:
        idx_pool = np.random.default_rng(42).choice(idx_pool, 4000, replace=False)
    sample = idx_pool[np.argsort(q50[idx_pool])]

    fig, ax = plt.subplots(figsize=(7.5, 5.0))
    ax.fill_between(range(len(sample)), q10[sample], q90[sample],
                    color="#1a6eb5", alpha=0.22, label="80% prediction interval (q10–q90)")
    ax.plot(range(len(sample)), q50[sample], color="#1a3a5c", lw=1.0, label="q50 median forecast")
    ax.scatter(range(len(sample)), y_true[sample], s=2, color="#e74c3c", alpha=0.5,
               label="Actual next-day level")
    ax.set_xlabel("Test sample (sorted by predicted median)", fontsize=10)
    ax.set_ylabel("Water level (m)", fontsize=10)
    ax.set_title(f"Quantile forecast vs actual — empirical 80% coverage = {coverage*100:.1f}%",
                 fontsize=11, color="#1a3a5c", weight="bold")
    ax.legend(loc="upper left", markerscale=3)
    ax.grid(alpha=0.25)
    ax.set_ylim(-0.5, 5)
    fig.tight_layout()
    fig.savefig(OUT_DIR / "quantile_coverage.png", dpi=140)
    plt.close(fig)

    # ====================================================== Alert timeline
    print("Plot 3: Alert tier timeline")
    df["alert"] = pd.Series([_classify_alert(p) for p in probs], index=df.index)
    daily = (
        df.groupby([df["date"].dt.date, "alert"]).size().unstack(fill_value=0)
    )
    for col in ["Normal", "Watch", "Alert", "Warning"]:
        if col not in daily.columns:
            daily[col] = 0
    daily = daily[["Normal", "Watch", "Alert", "Warning"]]
    fig, ax = plt.subplots(figsize=(10, 4.2))
    daily[["Watch", "Alert", "Warning"]].plot.area(
        ax=ax, stacked=True,
        color=["#f1c40f", "#e67e22", "#c0392b"], alpha=0.85,
    )
    ax.set_title("Stations in elevated alert tiers — daily count, 2023-2024",
                 fontsize=11, color="#1a3a5c", weight="bold")
    ax.set_ylabel("# stations", fontsize=10)
    ax.set_xlabel("")
    ax.grid(alpha=0.25)
    fig.tight_layout()
    fig.savefig(OUT_DIR / "alert_timeline.png", dpi=140)
    plt.close(fig)

    # ====================================================== Save metrics
    pinball = {}
    for q in QUANTILES:
        preds = q_preds[q]
        err = y_true - preds
        pinball[q] = float(np.mean(np.where(err >= 0, q * err, (q - 1) * err)))
    metrics = {
        "n_test_rows": int(len(df)),
        "flood_positive_rate": float(y_flood.mean()),
        "roc_auc": float(auc),
        "mae_q50": float(np.mean(np.abs(y_true - q50))),
        "coverage_80": float(coverage),
        "mean_interval_width_m": float(np.mean(q90 - q10)),
        "pinball_loss": {f"q{int(q*100):02d}": round(v, 5) for q, v in pinball.items()},
    }
    import json
    with open(OUT_DIR / "test_metrics.json", "w") as f:
        json.dump(metrics, f, indent=2)
    print(f"\nMetrics written → {OUT_DIR/'test_metrics.json'}")
    print(json.dumps(metrics, indent=2))


if __name__ == "__main__":
    main()
