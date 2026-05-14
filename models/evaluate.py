"""
Evaluation utilities for the flood prediction pipeline.

Metrics:
  - ROC-AUC     (discrimination)
  - PR-AUC / Average Precision  (performance under class imbalance)
  - F1 at the threshold that maximises F1 on the evaluation set
  - Classification report

Outputs:
  - Console summary
  - ROC and PR curve PNG files
  - JSON metrics file
"""

from __future__ import annotations

import json
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
from sklearn.metrics import (
    average_precision_score,
    classification_report,
    precision_recall_curve,
    roc_auc_score,
    roc_curve,
)


def evaluate(
    y_true: np.ndarray,
    y_prob: np.ndarray,
    model_name: str,
    save_dir: str | None = None,
) -> dict:
    """
    Compute metrics and optionally persist plots + JSON.

    Parameters
    ----------
    y_true     Binary ground truth (0/1).
    y_prob     Predicted probabilities in [0, 1].
    model_name Label used in file names and plot titles.
    save_dir   If given, save plots and metrics JSON here.
    """
    y_true = y_true.astype(int)

    roc_auc = roc_auc_score(y_true, y_prob)
    pr_auc = average_precision_score(y_true, y_prob)

    # Threshold that maximises F1 on this evaluation split
    precision, recall, thresholds = precision_recall_curve(y_true, y_prob)
    f1 = 2 * precision * recall / (precision + recall + 1e-9)
    best_idx = int(np.argmax(f1))
    best_thr = float(thresholds[best_idx]) if best_idx < len(thresholds) else 0.5
    y_pred = (y_prob >= best_thr).astype(int)

    metrics = {
        "model": model_name,
        "roc_auc": round(float(roc_auc), 6),
        "pr_auc": round(float(pr_auc), 6),
        "best_f1": round(float(f1[best_idx]), 6),
        "best_threshold": round(best_thr, 6),
        "positive_rate": round(float(y_true.mean()), 6),
        "n_samples": int(len(y_true)),
    }

    print(f"\n{'='*60}")
    print(f"  {model_name}")
    print(f"  ROC-AUC : {roc_auc:.4f}")
    print(f"  PR-AUC  : {pr_auc:.4f}")
    print(f"  Best F1 : {f1[best_idx]:.4f}  (threshold={best_thr:.3f})")
    print(classification_report(y_true, y_pred, target_names=["normal", "flood"]))

    if save_dir:
        Path(save_dir).mkdir(parents=True, exist_ok=True)
        _plot_roc(y_true, y_prob, model_name, save_dir)
        _plot_pr(precision, recall, pr_auc, model_name, save_dir)
        out = Path(save_dir) / f"{model_name}_metrics.json"
        with open(out, "w") as f:
            json.dump(metrics, f, indent=2)
        print(f"  Saved plots + metrics → {save_dir}")

    return metrics


def evaluate_quantile(
    y_true: np.ndarray,
    q_preds: dict,
    quantiles: list,
    exceedance_probs: np.ndarray,
    exceedance_true: np.ndarray,
    model_name: str,
    save_dir: str | None = None,
) -> dict:
    """
    Evaluate a quantile regression model.

    Metrics
    -------
    - Pinball loss per quantile
    - 80% interval coverage  (q10–q90 contains true value)
    - 80% interval width     (mean q90 − q10)
    - MAE at median          (q50 point forecast)
    - ROC-AUC for exceedance probability vs binary flood label
    """
    mask = ~np.isnan(y_true)
    y_true = y_true[mask]

    pinball = {}
    for q in quantiles:
        preds = q_preds[q][mask]
        err   = y_true - preds
        loss  = np.mean(np.where(err >= 0, q * err, (q - 1) * err))
        pinball[q] = round(float(loss), 6)

    q10 = q_preds[0.10][mask]
    q50 = q_preds[0.50][mask]
    q90 = q_preds[0.90][mask]

    coverage   = float(np.mean((y_true >= q10) & (y_true <= q90)))
    mean_width = float(np.mean(q90 - q10))
    mae_median = float(np.mean(np.abs(y_true - q50)))

    exceedance_mask = ~np.isnan(exceedance_probs) & ~np.isnan(exceedance_true.astype(float))
    roc_auc = float(roc_auc_score(
        exceedance_true[exceedance_mask].astype(int),
        exceedance_probs[exceedance_mask],
    )) if exceedance_mask.sum() > 0 else float("nan")

    metrics = {
        "model":          model_name,
        "pinball_loss":   pinball,
        "coverage_80pct": round(coverage, 4),
        "interval_width": round(mean_width, 4),
        "mae_median":     round(mae_median, 4),
        "roc_auc_exceedance": round(roc_auc, 6),
        "n_samples":      int(mask.sum()),
    }

    print(f"\n{'='*60}")
    print(f"  {model_name}")
    print(f"  ROC-AUC (exceedance) : {roc_auc:.4f}")
    print(f"  MAE at median (q50)  : {mae_median:.4f} m")
    print(f"  80% coverage         : {coverage*100:.1f}%  (ideal: 80%)")
    print(f"  80% interval width   : {mean_width:.4f} m")
    print("  Pinball loss by quantile:")
    for q, v in pinball.items():
        print(f"    q{int(q*100):02d}: {v:.5f}")

    if save_dir:
        Path(save_dir).mkdir(parents=True, exist_ok=True)
        _plot_quantile_coverage(y_true, q10, q50, q90, model_name, save_dir)
        out = Path(save_dir) / f"{model_name}_metrics.json"
        with open(out, "w") as f:
            json.dump(metrics, f, indent=2)
        print(f"  Saved plots + metrics → {save_dir}")

    return metrics


def _plot_quantile_coverage(y_true, q10, q50, q90, name, save_dir):
    """Scatter of predicted median vs actual with 80% interval shading."""
    idx = np.argsort(q50)[:5000]  # sample 5k for readability
    fig, ax = plt.subplots(figsize=(7, 5))
    ax.fill_between(
        range(len(idx)), q10[idx], q90[idx],
        alpha=0.25, color="#1f6feb", label="80% interval",
    )
    ax.plot(q50[idx], color="#1f6feb", lw=0.8, label="q50 forecast")
    ax.scatter(range(len(idx)), y_true[idx], s=1, color="#e74c3c", alpha=0.4, label="actual")
    ax.set_xlabel("Sample (sorted by q50)")
    ax.set_ylabel("Water level (m)")
    ax.set_title(f"Quantile coverage — {name}")
    ax.legend(markerscale=4)
    fig.tight_layout()
    fig.savefig(Path(save_dir) / f"{name}_coverage.png", dpi=120)
    plt.close(fig)


def _plot_roc(y_true, y_prob, name, save_dir):
    fpr, tpr, _ = roc_curve(y_true, y_prob)
    auc = roc_auc_score(y_true, y_prob)
    fig, ax = plt.subplots(figsize=(6, 5))
    ax.plot(fpr, tpr, lw=1.5, label=f"AUC = {auc:.4f}")
    ax.plot([0, 1], [0, 1], "k--", lw=0.8)
    ax.set_xlabel("False Positive Rate")
    ax.set_ylabel("True Positive Rate")
    ax.set_title(f"ROC — {name}")
    ax.legend()
    fig.tight_layout()
    fig.savefig(Path(save_dir) / f"{name}_roc.png", dpi=120)
    plt.close(fig)


def _plot_pr(precision, recall, ap, name, save_dir):
    fig, ax = plt.subplots(figsize=(6, 5))
    ax.plot(recall, precision, lw=1.5, label=f"AP = {ap:.4f}")
    ax.set_xlabel("Recall")
    ax.set_ylabel("Precision")
    ax.set_title(f"Precision-Recall — {name}")
    ax.legend()
    fig.tight_layout()
    fig.savefig(Path(save_dir) / f"{name}_pr.png", dpi=120)
    plt.close(fig)
