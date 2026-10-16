"""Isotonic calibration: a detector's score -> P(fraud | score).

Fitted on the validation split and stored in the model's metadata, so every
risk signal means the same thing (a fraud probability) whatever the detector
(LightGBM probability, Isolation Forest percentile). The mapping is monotone,
so it never changes a detector's ranking.
"""

from __future__ import annotations

from typing import Any

import numpy as np
from sklearn.isotonic import IsotonicRegression


def fit(scores: np.ndarray, y: np.ndarray) -> dict[str, list[float]]:
    iso = IsotonicRegression(y_min=0.0, y_max=1.0, out_of_bounds="clip").fit(scores, y)
    return {
        "x": [round(float(v), 8) for v in iso.X_thresholds_],
        "y": [round(float(v), 8) for v in iso.y_thresholds_],
    }


def apply(calibration: dict[str, Any], scores: np.ndarray) -> np.ndarray:
    return np.interp(scores, calibration["x"], calibration["y"])
