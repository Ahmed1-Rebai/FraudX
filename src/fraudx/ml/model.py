"""Online inference: loads a versioned artifact and scores feature vectors.

Training (``fraudx.ml.train``) and inference are separate: the scorer only
needs this module, the artifact directory and ``fraudx.ml.vectorize``.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import lightgbm as lgb
import numpy as np

from fraudx.core.schemas import FEATURE_VERSION, TransactionEvent, TransactionFeatures
from fraudx.ml.vectorize import FEATURE_NAMES, vectorize


class IncompatibleModelError(RuntimeError):
    pass


@dataclass(frozen=True)
class FraudModel:
    booster: lgb.Booster
    metadata: dict[str, Any]

    @property
    def version(self) -> str:
        return str(self.metadata["version"])

    @property
    def threshold(self) -> float:
        return float(self.metadata["threshold"])

    @classmethod
    def load(cls, artifact_dir: Path) -> FraudModel:
        metadata = json.loads((artifact_dir / "metadata.json").read_text(encoding="utf-8"))
        if metadata["feature_version"] != FEATURE_VERSION:
            raise IncompatibleModelError(
                f"model expects feature v{metadata['feature_version']}, "
                f"pipeline produces v{FEATURE_VERSION}"
            )
        if tuple(metadata["feature_names"]) != FEATURE_NAMES:
            raise IncompatibleModelError("model feature names differ from fraudx.ml.vectorize")
        booster = lgb.Booster(model_file=str(artifact_dir / "model.txt"))
        return cls(booster, metadata)

    def predict_vectors(self, rows: list[list[float]]) -> np.ndarray:
        return self.booster.predict(np.asarray(rows, dtype=np.float32), num_threads=1)

    def predict(self, event: TransactionEvent, features: TransactionFeatures) -> float:
        """Fraud probability in [0, 1] for one transaction."""
        return float(self.predict_vectors([vectorize(event, features)])[0])
