import os
import sys

# Tests run against the source tree directly (no install step in this
# workflow), so make sure `backoffaudit` resolves from src/ either way.
_SRC = os.path.join(os.path.dirname(__file__), "..", "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)
