import sys
from pathlib import Path

_COMEX_IND_ROOT = Path(__file__).resolve().parent
if str(_COMEX_IND_ROOT) not in sys.path:
    sys.path.insert(0, str(_COMEX_IND_ROOT))
