from __future__ import annotations

import sys
from pathlib import Path

# Ensure project root (OptionsPredicator/) is on sys.path so `import options_ai` works.
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
