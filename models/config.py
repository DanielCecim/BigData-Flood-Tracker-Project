from dataclasses import dataclass
from pathlib import Path

ROOT_DIR = Path(__file__).parent.parent


@dataclass
class Config:
    # --------------- paths ---------------
    csv_path: str = str(ROOT_DIR / "exports" / "ml_dataset.csv")
    artifacts_dir: str = str(ROOT_DIR / "models" / "artifacts")

    # --------------- temporal splits ---------------
    # Train  : up to and including train_end_date
    # Val    : (train_end_date, val_end_date]
    # Test   : everything after val_end_date
    train_end_date: str = "2020-12-31"
    val_end_date: str = "2022-12-31"

    # --------------- target ---------------
    flood_percentile: float = 90.0   # threshold = per-station Pth percentile

    # --------------- feature windows ---------------
    river_lag_days: tuple = (1, 2, 3, 7, 14, 30)
    weather_lag_days: tuple = (1, 2, 3, 7)
    river_rolling_windows: tuple = (7, 14, 30)
    precip_rolling_windows: tuple = (3, 7, 14)

    # --------------- LightGBM ---------------
    lgbm_n_estimators: int = 5000
    lgbm_learning_rate: float = 0.01
    lgbm_num_leaves: int = 63
    lgbm_max_depth: int = 7
    lgbm_min_child_samples: int = 100
    lgbm_subsample: float = 0.8
    lgbm_colsample_bytree: float = 0.8
    lgbm_early_stopping_rounds: int = 200
    lgbm_log_period: int = 200

    # --------------- LSTM ---------------
    lstm_seq_len: int = 30          # look-back window (days)
    lstm_hidden_size: int = 128
    lstm_num_layers: int = 2
    lstm_dropout: float = 0.3
    lstm_batch_size: int = 1024
    lstm_epochs: int = 50
    lstm_lr: float = 1e-3
    lstm_weight_decay: float = 1e-5
    lstm_patience: int = 10         # early-stopping patience

    # --------------- misc ---------------
    random_seed: int = 42
