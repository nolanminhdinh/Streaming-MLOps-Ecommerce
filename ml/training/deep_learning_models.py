"""
deep_learning_models.py
-----------------------
Kiến trúc mạng nơ-ron hồi quy (Deep Learning Recurrent Networks) cho bài toán dự báo nhu cầu:
  1. LSTM (Long Short-Term Memory) Network:
     - Đa tầng (Multi-layer LSTM) kèm Dropout chống Overfitting.
     - Lớp Fully Connected (FC) với hàm kích hoạt ReLU/Softplus đảm bảo dự báo sản lượng không âm (Demand >= 0).
  2. GRU (Gated Recurrent Unit) Network:
     - Cấu trúc cổng rút gọn, huấn luyện nhanh hơn, phù hợp cho chuỗi dữ liệu vừa và nhỏ.
  3. TimeSeriesSequenceDataset:
     - Tạo cửa sổ trượt (Sliding Windows) chuyển dữ liệu dạng bảng 2D thành tensor 3D: (Batch, Seq_Len, Features).
  4. DeepLearningTrainer:
     - Vòng lặp huấn luyện tối ưu với Huber Loss (Smooth L1 - chống sốc ngoại lai Flash Sale).
     - Bộ tối ưu AdamW, Early Stopping và điều chỉnh Learning Rate tự động (ReduceLROnPlateau).

Tuần 6: Huấn luyện mô hình Deep Learning cho chuỗi thời gian.
"""

from __future__ import annotations

import logging
import os
import sys
from typing import Dict, List, Optional, Tuple, Union

try:
    import numpy as np
except ImportError:
    np = None

logger = logging.getLogger("ml.deep_learning")

# Kiểm tra thư viện PyTorch
try:
    import torch
    import torch.nn as nn
    from torch.utils.data import DataLoader, Dataset
    HAS_TORCH = True
except ImportError:
    HAS_TORCH = False
    nn = object
    Dataset = object
    logger.warning("PyTorch chưa được cài đặt. Module Deep Learning sẽ hoạt động ở chế độ Fallback.")


# ─────────────────────────────────────────────────────────────
# 1. DATASET: SLIDING WINDOW TENSOR GENERATOR
# ─────────────────────────────────────────────────────────────

class TimeSeriesSequenceDataset(Dataset if HAS_TORCH else object):
    """
    Chuyển đổi chuỗi dữ liệu dạng bảng thành các cửa sổ trượt 3D:
      Input X: (N, seq_len, n_features)
      Target y: (N, 1)
    """

    def __init__(
        self,
        features: np.ndarray,
        targets: np.ndarray,
        seq_len: int = 14,
    ):
        self.seq_len = seq_len
        self.X_windows, self.y_windows = self._create_windows(features, targets, seq_len)

    def _create_windows(
        self,
        features: np.ndarray,
        targets: np.ndarray,
        seq_len: int,
    ) -> Tuple[np.ndarray, np.ndarray]:
        X_list, y_list = [], []
        n_samples = len(features)

        if n_samples <= seq_len:
            # Fallback nếu dữ liệu quá ngắn
            pad_len = seq_len - n_samples + 1
            padded_feat = np.pad(features, ((pad_len, 0), (0, 0)), mode="edge")
            padded_target = np.pad(targets, (pad_len, 0), mode="edge")
            return np.array([padded_feat[:seq_len]]), np.array([padded_target[seq_len]])

        for i in range(n_samples - seq_len):
            X_list.append(features[i : i + seq_len])
            y_list.append(targets[i + seq_len])

        return np.array(X_list, dtype=np.float32), np.array(y_list, dtype=np.float32).reshape(-1, 1)

    def __len__(self):
        return len(self.X_windows)

    def __getitem__(self, idx):
        if HAS_TORCH:
            return (
                torch.tensor(self.X_windows[idx], dtype=torch.float32),
                torch.tensor(self.y_windows[idx], dtype=torch.float32),
            )
        return self.X_windows[idx], self.y_windows[idx]


# ─────────────────────────────────────────────────────────────
# 2. KIẾN TRÚC MẠNG LSTM & GRU
# ─────────────────────────────────────────────────────────────

if HAS_TORCH:
    class DemandLSTM(nn.Module):
        """Mạng LSTM hồi quy đa biến cho dự báo nhu cầu hàng hóa."""

        def __init__(
            self,
            input_dim: int,
            hidden_dim: int = 64,
            num_layers: int = 2,
            dropout: float = 0.2,
        ):
            super().__init__()
            self.hidden_dim = hidden_dim
            self.num_layers = num_layers

            self.lstm = nn.LSTM(
                input_size=input_dim,
                hidden_size=hidden_dim,
                num_layers=num_layers,
                batch_first=True,
                dropout=dropout if num_layers > 1 else 0.0,
            )

            self.fc_head = nn.Sequential(
                nn.Linear(hidden_dim, 32),
                nn.ReLU(),
                nn.Dropout(dropout),
                nn.Linear(32, 1),
                nn.ReLU(),  # Đảm bảo nhu cầu dự báo luôn >= 0
            )

        def forward(self, x):
            # x shape: (batch_size, seq_len, input_dim)
            lstm_out, _ = self.lstm(x)
            # Lấy vector ẩn tại bước thời gian cuối cùng: (batch_size, hidden_dim)
            last_hidden = lstm_out[:, -1, :]
            out = self.fc_head(last_hidden)
            return out


    class DemandGRU(nn.Module):
        """Mạng GRU hồi quy đa biến cho dự báo nhu cầu hàng hóa."""

        def __init__(
            self,
            input_dim: int,
            hidden_dim: int = 64,
            num_layers: int = 2,
            dropout: float = 0.2,
        ):
            super().__init__()
            self.hidden_dim = hidden_dim
            self.num_layers = num_layers

            self.gru = nn.GRU(
                input_size=input_dim,
                hidden_size=hidden_dim,
                num_layers=num_layers,
                batch_first=True,
                dropout=dropout if num_layers > 1 else 0.0,
            )

            self.fc_head = nn.Sequential(
                nn.Linear(hidden_dim, 32),
                nn.ReLU(),
                nn.Dropout(dropout),
                nn.Linear(32, 1),
                nn.ReLU(),
            )

        def forward(self, x):
            gru_out, _ = self.gru(x)
            last_hidden = gru_out[:, -1, :]
            out = self.fc_head(last_hidden)
            return out

else:
    # Dummy mock classes khi chưa có PyTorch
    class DemandLSTM:
        def __init__(self, input_dim: int, hidden_dim: int = 64, num_layers: int = 2, dropout: float = 0.2):
            self.input_dim = input_dim

    class DemandGRU:
        def __init__(self, input_dim: int, hidden_dim: int = 64, num_layers: int = 2, dropout: float = 0.2):
            self.input_dim = input_dim


# ─────────────────────────────────────────────────────────────
# 3. DEEP LEARNING TRAINER
# ─────────────────────────────────────────────────────────────

class DeepLearningTrainer:
    """Quản lý chu trình huấn luyện, early stopping và suy luận cho mô hình Deep Learning."""

    def __init__(
        self,
        model_type: str = "lstm",
        seq_len: int = 14,
        hidden_dim: int = 64,
        num_layers: int = 2,
        dropout: float = 0.2,
        lr: float = 0.001,
        batch_size: int = 32,
        max_epochs: int = 40,
        patience: int = 7,
        device: Optional[str] = None,
    ):
        self.model_type = model_type.lower()
        self.seq_len = seq_len
        self.hidden_dim = hidden_dim
        self.num_layers = num_layers
        self.dropout = dropout
        self.lr = lr
        self.batch_size = batch_size
        self.max_epochs = max_epochs
        self.patience = patience

        if HAS_TORCH:
            if device:
                self.device = torch.device(device)
            else:
                self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        else:
            self.device = "cpu"

        self.model = None
        self.input_dim = None

    def _init_network(self, input_dim: int):
        self.input_dim = input_dim
        if not HAS_TORCH:
            return

        if self.model_type == "gru":
            self.model = DemandGRU(
                input_dim=input_dim,
                hidden_dim=self.hidden_dim,
                num_layers=self.num_layers,
                dropout=self.dropout,
            ).to(self.device)
        else:
            self.model = DemandLSTM(
                input_dim=input_dim,
                hidden_dim=self.hidden_dim,
                num_layers=self.num_layers,
                dropout=self.dropout,
            ).to(self.device)

    def fit(
        self,
        X_train: np.ndarray,
        y_train: np.ndarray,
        X_val: Optional[np.ndarray] = None,
        y_val: Optional[np.ndarray] = None,
    ) -> Dict[str, List[float]]:
        """
        Huấn luyện mạng nơ-ron trên tập dữ liệu chuỗi thời gian.
        """
        if not HAS_TORCH:
            logger.warning("Bỏ qua training PyTorch thực tế do chưa cài đặt torch.")
            return {"train_loss": [0.0], "val_loss": [0.0]}

        self._init_network(input_dim=X_train.shape[1])

        train_dataset = TimeSeriesSequenceDataset(X_train, y_train, seq_len=self.seq_len)
        train_loader = DataLoader(train_dataset, batch_size=self.batch_size, shuffle=True)

        has_val = X_val is not None and y_val is not None and len(X_val) > self.seq_len
        if has_val:
            val_dataset = TimeSeriesSequenceDataset(X_val, y_val, seq_len=self.seq_len)
            val_loader = DataLoader(val_dataset, batch_size=self.batch_size, shuffle=False)

        # Sử dụng Huber Loss (Smooth L1 Loss) để kháng ngoại lai trong mùa Flash Sale
        criterion = nn.SmoothL1Loss()
        optimizer = torch.optim.AdamW(self.model.parameters(), lr=self.lr, weight_decay=1e-4)
        scheduler = torch.optim.lr_scheduler.ReduceLROnPlateau(optimizer, mode="min", factor=0.5, patience=3)

        history = {"train_loss": [], "val_loss": []}
        best_val_loss = float("inf")
        patience_counter = 0

        for epoch in range(1, self.max_epochs + 1):
            self.model.train()
            train_loss = 0.0

            for batch_X, batch_y in train_loader:
                batch_X, batch_y = batch_X.to(self.device), batch_y.to(self.device)
                optimizer.zero_grad()
                outputs = self.model(batch_X)
                loss = criterion(outputs, batch_y)
                loss.backward()
                torch.nn.utils.clip_grad_norm_(self.model.parameters(), max_norm=1.0)
                optimizer.step()
                train_loss += loss.item() * len(batch_X)

            avg_train_loss = train_loss / len(train_dataset) if len(train_dataset) > 0 else 0.0
            history["train_loss"].append(avg_train_loss)

            # Đánh giá trên tập validation
            avg_val_loss = avg_train_loss
            if has_val:
                self.model.eval()
                val_loss = 0.0
                with torch.no_grad():
                    for batch_X, batch_y in val_loader:
                        batch_X, batch_y = batch_X.to(self.device), batch_y.to(self.device)
                        preds = self.model(batch_X)
                        val_loss += criterion(preds, batch_y).item() * len(batch_X)
                avg_val_loss = val_loss / len(val_dataset) if len(val_dataset) > 0 else 0.0
                scheduler.step(avg_val_loss)

            history["val_loss"].append(avg_val_loss)

            # Early Stopping
            if avg_val_loss < best_val_loss:
                best_val_loss = avg_val_loss
                patience_counter = 0
            else:
                patience_counter += 1
                if patience_counter >= self.patience:
                    logger.debug("Early stopping kích hoạt tại epoch %d (Best Val Loss: %.4f)", epoch, best_val_loss)
                    break

        return history

    def predict(self, X: np.ndarray) -> np.ndarray:
        """Thực hiện suy luận dự báo nhu cầu."""
        if not HAS_TORCH or self.model is None:
            # Fallback heuristic: lấy cột feature đầu tiên
            return np.maximum(0.0, X[:, 0] if X.ndim > 1 else X)

        self.model.eval()
        dummy_targets = np.zeros(len(X))
        dataset = TimeSeriesSequenceDataset(X, dummy_targets, seq_len=self.seq_len)
        loader = DataLoader(dataset, batch_size=self.batch_size, shuffle=False)

        all_preds = []
        with torch.no_grad():
            for batch_X, _ in loader:
                batch_X = batch_X.to(self.device)
                preds = self.model(batch_X)
                all_preds.extend(preds.cpu().numpy().flatten())

        # Nếu tập dữ liệu ban đầu dài hơn số lượng windows sinh ra, bổ sung bằng giá trị đầu
        preds_array = np.array(all_preds)
        if len(preds_array) < len(X):
            pad = np.full(len(X) - len(preds_array), preds_array[0] if len(preds_array) > 0 else 0.0)
            preds_array = np.concatenate([pad, preds_array])

        return np.maximum(0.0, preds_array)
