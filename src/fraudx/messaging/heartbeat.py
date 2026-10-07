"""Container healthcheck for workers: is the heartbeat file fresh?

python -m fraudx.messaging.heartbeat /tmp/fraudx/heartbeat 30
"""

from __future__ import annotations

import sys
import time
from pathlib import Path


def is_fresh(path: Path, max_age_s: float) -> bool:
    try:
        return time.time() - path.stat().st_mtime <= max_age_s
    except FileNotFoundError:
        return False


if __name__ == "__main__":
    if len(sys.argv) < 2:
        sys.exit("usage: python -m fraudx.messaging.heartbeat <file> [max-age-seconds]")
    max_age = float(sys.argv[2]) if len(sys.argv) > 2 else 30.0
    sys.exit(0 if is_fresh(Path(sys.argv[1]), max_age) else 1)
