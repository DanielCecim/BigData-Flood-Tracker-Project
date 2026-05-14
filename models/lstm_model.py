"""
LSTM flood classifier.

Architecture:
  Input : (batch, seq_len, n_features) — raw daily values for the last seq_len days
  LSTM  : multi-layer with dropout
  Head  : LayerNorm → Dropout → Linear(1) — outputs a single logit

The FloodSequenceDataset generates sequences lazily from a pre-loaded
numpy matrix; it never duplicates data in memory.

Sequence validity: a row can be a sequence end only when the preceding
seq_len rows belong to the same station with no date gaps.
"""

from __future__ import annotations

import pickle
from pathlib import Path
from typing import List, Tuple

import numpy as np
import pandas as pd
import torch
import torch.nn as nn
from torch.utils.data import DataLoader, Dataset

from config import Config
from feature_engineering import LSTM_FEATURE_COLS


# ------------------------------------------------------------------ #
#  Sequence validity detection (vectorised, no Python loops)
# ------------------------------------------------------------------ #

def find_valid_sequence_ends(df: pd.DataFrame, seq_len: int) -> List[int]:
    """
    Return iloc positions (0-based) that can serve as the end of a
    complete seq_len-day window within a single station.
    """
    same_station = (df["station_ref"] == df["station_ref"].shift(1))
    consec_day = (df["date"] - df["date"].shift(1)).dt.days == 1
    is_consec = (same_station & consec_day).astype(np.int8)

    # Rolling min over [t-(seq_len-1), t]: if all 1 the window is complete
    full_window = (
        is_consec
        .rolling(seq_len - 1, min_periods=seq_len - 1)
        .min()
        .fillna(0)
        .astype(bool)
    )
    has_target = df["target"].notna()
    return df.index[full_window & has_target].tolist()


# ------------------------------------------------------------------ #
#  Dataset
# ------------------------------------------------------------------ #

class FloodSequenceDataset(Dataset):
    """Memory-efficient dataset: features stored once, sequences sliced on demand."""

    def __init__(
        self,
        X: np.ndarray,       # (N, n_features) float32
        y: np.ndarray,       # (N,)            float32
        valid_ends: List[int],
        seq_len: int,
    ):
        self.X = X
        self.y = y
        self.valid_ends = valid_ends
        self.seq_len = seq_len

    def __len__(self) -> int:
        return len(self.valid_ends)

    def __getitem__(self, idx: int) -> Tuple[torch.Tensor, torch.Tensor]:
        end = self.valid_ends[idx]
        x_seq = torch.from_numpy(self.X[end - self.seq_len : end])  # (seq_len, F)
        y_val = torch.tensor(self.y[end], dtype=torch.float32)
        return x_seq, y_val


# ------------------------------------------------------------------ #
#  Model
# ------------------------------------------------------------------ #

class LSTMFloodModel(nn.Module):
    def __init__(
        self,
        input_size: int,
        hidden_size: int,
        num_layers: int,
        dropout: float,
    ):
        super().__init__()
        self.lstm = nn.LSTM(
            input_size=input_size,
            hidden_size=hidden_size,
            num_layers=num_layers,
            dropout=dropout if num_layers > 1 else 0.0,
            batch_first=True,
        )
        self.head = nn.Sequential(
            nn.LayerNorm(hidden_size),
            nn.Dropout(dropout),
            nn.Linear(hidden_size, 1),
        )

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        # x: (B, seq_len, input_size)
        out, _ = self.lstm(x)
        return self.head(out[:, -1, :]).squeeze(-1)  # (B,) logits


# ------------------------------------------------------------------ #
#  Trainer
# ------------------------------------------------------------------ #

class LSTMTrainer:
    def __init__(self, config: Config):
        self.config = config
        self.model: LSTMFloodModel | None = None
        self.scaler_mean: np.ndarray | None = None
        self.scaler_std: np.ndarray | None = None
        self._best_state: dict | None = None
        self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        print(f"  LSTM device: {self.device}")

    # ---- scaling ----

    def _scale(self, X: np.ndarray, fit: bool = False) -> np.ndarray:
        if fit:
            self.scaler_mean = np.nanmean(X, axis=0)
            self.scaler_std = np.nanstd(X, axis=0) + 1e-8
        return ((X - self.scaler_mean) / self.scaler_std).astype(np.float32)

    # ---- dataset builder ----

    def _make_dataset(
        self, df: pd.DataFrame, X_scaled: np.ndarray
    ) -> FloodSequenceDataset:
        valid_ends = find_valid_sequence_ends(df, self.config.lstm_seq_len)
        y = df["target"].values.astype(np.float32)
        return FloodSequenceDataset(X_scaled, y, valid_ends, self.config.lstm_seq_len)

    # ---- training ----

    def fit(self, train_df: pd.DataFrame, val_df: pd.DataFrame) -> dict:
        feat_cols = [c for c in LSTM_FEATURE_COLS if c in train_df.columns]

        train_df = train_df.reset_index(drop=True)
        val_df = val_df.reset_index(drop=True)

        X_tr = self._scale(train_df[feat_cols].values.astype(np.float32), fit=True)
        X_va = self._scale(val_df[feat_cols].values.astype(np.float32), fit=False)

        train_ds = self._make_dataset(train_df, X_tr)
        val_ds = self._make_dataset(val_df, X_va)
        print(
            f"  Sequences — train: {len(train_ds):,} | val: {len(val_ds):,}"
        )

        # Weighted loss for class imbalance
        pos_rate = float(
            train_df["target"].dropna().mean()
        )
        pos_weight = torch.tensor(
            (1 - pos_rate) / pos_rate, device=self.device
        )
        criterion = nn.BCEWithLogitsLoss(pos_weight=pos_weight)

        self.model = LSTMFloodModel(
            input_size=len(feat_cols),
            hidden_size=self.config.lstm_hidden_size,
            num_layers=self.config.lstm_num_layers,
            dropout=self.config.lstm_dropout,
        ).to(self.device)

        optimizer = torch.optim.AdamW(
            self.model.parameters(),
            lr=self.config.lstm_lr,
            weight_decay=self.config.lstm_weight_decay,
        )
        scheduler = torch.optim.lr_scheduler.ReduceLROnPlateau(
            optimizer, patience=3, factor=0.5
        )

        pin = self.device.type == "cuda"
        train_loader = DataLoader(
            train_ds,
            batch_size=self.config.lstm_batch_size,
            shuffle=True,
            num_workers=0,
            pin_memory=pin,
        )
        val_loader = DataLoader(
            val_ds,
            batch_size=self.config.lstm_batch_size * 2,
            shuffle=False,
            num_workers=0,
        )

        best_val = float("inf")
        patience_ctr = 0
        history: dict = {"train_loss": [], "val_loss": []}

        for epoch in range(1, self.config.lstm_epochs + 1):
            # -- train --
            self.model.train()
            tr_losses = []
            for X_batch, y_batch in train_loader:
                X_batch = X_batch.to(self.device)
                y_batch = y_batch.to(self.device)
                optimizer.zero_grad()
                loss = criterion(self.model(X_batch), y_batch)
                loss.backward()
                nn.utils.clip_grad_norm_(self.model.parameters(), 1.0)
                optimizer.step()
                tr_losses.append(loss.item())

            # -- validate --
            self.model.eval()
            va_losses = []
            with torch.no_grad():
                for X_batch, y_batch in val_loader:
                    X_batch = X_batch.to(self.device)
                    y_batch = y_batch.to(self.device)
                    va_losses.append(criterion(self.model(X_batch), y_batch).item())

            tr_loss = float(np.mean(tr_losses))
            va_loss = float(np.mean(va_losses))
            history["train_loss"].append(tr_loss)
            history["val_loss"].append(va_loss)
            scheduler.step(va_loss)

            print(
                f"  Epoch {epoch:3d}/{self.config.lstm_epochs} "
                f"| train={tr_loss:.4f} val={va_loss:.4f}"
            )

            if va_loss < best_val - 1e-5:
                best_val = va_loss
                patience_ctr = 0
                self._best_state = {
                    k: v.cpu().clone() for k, v in self.model.state_dict().items()
                }
            else:
                patience_ctr += 1
                if patience_ctr >= self.config.lstm_patience:
                    print(f"  Early stopping triggered at epoch {epoch}")
                    break

        self.model.load_state_dict(self._best_state)
        return history

    # ---- inference ----

    def predict_proba(
        self, df: pd.DataFrame
    ) -> Tuple[np.ndarray, np.ndarray]:
        """
        Returns:
            probs   : shape (n_valid,) predicted probabilities
            indices : shape (n_valid,) iloc positions in df for each prediction
        """
        feat_cols = [c for c in LSTM_FEATURE_COLS if c in df.columns]
        df = df.reset_index(drop=True)

        X_scaled = self._scale(
            df[feat_cols].values.astype(np.float32), fit=False
        )
        valid_ends = find_valid_sequence_ends(df, self.config.lstm_seq_len)

        if not valid_ends:
            return np.array([]), np.array([], dtype=int)

        y_dummy = np.zeros(len(df), dtype=np.float32)
        ds = FloodSequenceDataset(
            X_scaled, y_dummy, valid_ends, self.config.lstm_seq_len
        )
        loader = DataLoader(
            ds,
            batch_size=self.config.lstm_batch_size * 2,
            shuffle=False,
            num_workers=0,
        )

        self.model.eval()
        probs = []
        with torch.no_grad():
            for X_batch, _ in loader:
                X_batch = X_batch.to(self.device)
                probs.append(
                    torch.sigmoid(self.model(X_batch)).cpu().numpy()
                )

        return np.concatenate(probs), np.array(valid_ends, dtype=int)

    # ---- persistence ----

    def save(self, path: str) -> None:
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "state_dict": {
                k: v.cpu() for k, v in self.model.state_dict().items()
            },
            "scaler_mean": self.scaler_mean,
            "scaler_std": self.scaler_std,
            "config": self.config,
            "input_size": self.model.lstm.input_size,
        }
        with open(path, "wb") as f:
            pickle.dump(payload, f)
        print(f"  LSTM model saved → {path}")

    @classmethod
    def load(cls, path: str) -> "LSTMTrainer":
        with open(path, "rb") as f:
            data = pickle.load(f)
        obj = cls(data["config"])
        obj.scaler_mean = data["scaler_mean"]
        obj.scaler_std = data["scaler_std"]
        cfg = data["config"]
        obj.model = LSTMFloodModel(
            input_size=data["input_size"],
            hidden_size=cfg.lstm_hidden_size,
            num_layers=cfg.lstm_num_layers,
            dropout=cfg.lstm_dropout,
        )
        obj.model.load_state_dict(data["state_dict"])
        obj.model.to(obj.device)
        return obj
