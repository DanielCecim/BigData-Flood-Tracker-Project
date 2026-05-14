"""
Modal cloud training runner for the UK flood prediction pipeline.

Setup (one-time):
    pip install modal
    modal setup                    # authenticate
    modal volume create floods-uk-data
    modal volume create floods-uk-artifacts

Upload training data (one-time):
    modal volume put floods-uk-data exports/ml_dataset.csv /ml_dataset.csv

Train:
    modal run models/modal_app.py --model lgbm
    modal run models/modal_app.py --model lstm
    modal run models/modal_app.py --model both

Download artifacts:
    modal volume get floods-uk-artifacts lgbm_model.pkl models/artifacts/lgbm_model.pkl
    modal volume get floods-uk-artifacts lstm_model.pkl models/artifacts/lstm_model.pkl
    modal volume get floods-uk-artifacts thresholds.json models/artifacts/thresholds.json
"""

from __future__ import annotations

from pathlib import Path

import modal

# ------------------------------------------------------------------ #
#  Infrastructure
# ------------------------------------------------------------------ #

app = modal.App("floods-uk-ml")

_models_dir = Path(__file__).parent

image = (
    modal.Image.debian_slim(python_version="3.11")
    .pip_install(
        "lightgbm>=4.3",
        "torch>=2.1",
        "scikit-learn>=1.4",
        "pandas>=2.1",
        "numpy>=1.24",
        "matplotlib>=3.7",
    )
    # Embed the models/ source tree into the image at build time (Modal >= 1.0).
    .add_local_dir(str(_models_dir), "/app/models")
)

data_vol = modal.Volume.from_name("floods-uk-data")
artifacts_vol = modal.Volume.from_name("floods-uk-artifacts")

DATA_MOUNT = "/vol/data"
ARTIFACTS_MOUNT = "/vol/artifacts"


# ------------------------------------------------------------------ #
#  Helpers shared between remote functions
# ------------------------------------------------------------------ #

def _build_config(percentile: float):
    """Construct a Config pointing at the Modal volume paths."""
    import sys
    sys.path.insert(0, "/app/models")
    from config import Config
    return Config(
        csv_path=f"{DATA_MOUNT}/ml_dataset.csv",
        artifacts_dir=ARTIFACTS_MOUNT,
        flood_percentile=percentile,
    )


# ------------------------------------------------------------------ #
#  Remote training functions
# ------------------------------------------------------------------ #

@app.function(
    image=image,
    cpu=8.0,
    memory=32768,            # 32 GB — feature matrix peaks ~3 GB
    volumes={
        DATA_MOUNT: data_vol,
        ARTIFACTS_MOUNT: artifacts_vol,
    },
    timeout=14_400,          # 4 hours
)
def train_lgbm_remote(percentile: float = 90.0) -> dict:
    import sys
    sys.path.insert(0, "/app/models")

    from train import train_lgbm
    metrics = train_lgbm(_build_config(percentile))

    # Flush writes to the persistent volume
    artifacts_vol.commit()
    return metrics


@app.function(
    image=image,
    cpu=8.0,
    memory=32768,
    volumes={
        DATA_MOUNT: data_vol,
        ARTIFACTS_MOUNT: artifacts_vol,
    },
    timeout=14_400,
)
def train_quantile_remote(percentile: float = 90.0) -> dict:
    import sys
    from pathlib import Path
    sys.path.insert(0, "/app/models")

    from train import train_quantile
    metrics = train_quantile(_build_config(percentile))

    # Also save a percentile-suffixed copy so reruns don't overwrite each other
    import shutil
    p = int(percentile)
    src = Path(ARTIFACTS_MOUNT) / "quantile_model.pkl"
    shutil.copy(src, Path(ARTIFACTS_MOUNT) / f"quantile_model_p{p}.pkl")
    shutil.copy(
        Path(ARTIFACTS_MOUNT) / "thresholds.json",
        Path(ARTIFACTS_MOUNT) / f"thresholds_p{p}.json",
    )

    artifacts_vol.commit()
    return metrics


@app.function(
    image=image,
    gpu="A10G",              # 24 GB VRAM — ample for (1024, 30, 12) LSTM batches
    memory=32768,
    volumes={
        DATA_MOUNT: data_vol,
        ARTIFACTS_MOUNT: artifacts_vol,
    },
    timeout=14_400,
)
def train_lstm_remote(percentile: float = 90.0) -> dict:
    import sys
    sys.path.insert(0, "/app/models")

    from train import train_lstm
    metrics = train_lstm(_build_config(percentile))

    artifacts_vol.commit()
    return metrics


# ------------------------------------------------------------------ #
#  Local entry point
# ------------------------------------------------------------------ #

@app.local_entrypoint()
def main(
    model: str = "lgbm",
    percentile: float = 90.0,
):
    """
    Dispatch training jobs to Modal.

    Args:
        model      : lgbm | lstm | quantile | both
        percentile : flood threshold percentile (default 90)
    """
    if model == "lgbm":
        metrics = train_lgbm_remote.remote(percentile=percentile)
        print("LightGBM metrics:", metrics)

    elif model == "lstm":
        metrics = train_lstm_remote.remote(percentile=percentile)
        print("LSTM metrics:", metrics)

    elif model == "quantile":
        metrics = train_quantile_remote.remote(percentile=percentile)
        print("Quantile metrics:", metrics)

    elif model == "both":
        lgbm_h     = train_lgbm_remote.spawn(percentile=percentile)
        lstm_h     = train_lstm_remote.spawn(percentile=percentile)
        quantile_h = train_quantile_remote.spawn(percentile=percentile)
        print("LightGBM metrics:", lgbm_h.get())
        print("LSTM metrics    :", lstm_h.get())
        print("Quantile metrics:", quantile_h.get())

    else:
        raise ValueError(f"Unknown model '{model}'. Choose lgbm | lstm | quantile | both.")
