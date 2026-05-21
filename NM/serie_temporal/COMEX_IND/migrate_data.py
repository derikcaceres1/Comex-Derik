"""One-time migration: copy scraper parquet artifacts and proxies.csv from the legacy
``Mario/Comex/COMEX_IND`` tree into the NM-managed ``NM/dados/IND`` tree.

Run once from the GEP root:
    python Comex-Derik/NM/serie_temporal/COMEX_IND/migrate_data.py
"""

import shutil
import sys
from pathlib import Path


_THIS = Path(__file__).resolve()
_GEP_ROOT = _THIS.parents[4]

SRC_PARQUET = _GEP_ROOT / "Mario" / "Comex" / "COMEX_IND" / "data" / "parquet"
DST_PARQUET = _GEP_ROOT / "Comex-Derik" / "NM" / "dados" / "IND" / "scraper_parquet"

SRC_PROXIES = _GEP_ROOT / "Mario" / "Comex" / "COMEX_IND" / "proxies.csv"
DST_PROXIES = _GEP_ROOT / "Comex-Derik" / "NM" / "dados" / "IND" / "proxies.csv"


def copy_parquet_tree() -> int:
    """Copy new parquet files from the legacy tree to the NM-managed directory; return count copied."""
    if not SRC_PARQUET.exists():
        print(f"ERROR: source parquet tree not found: {SRC_PARQUET}", file=sys.stderr)
        return 0
    DST_PARQUET.mkdir(parents=True, exist_ok=True)
    copied = 0
    for src_file in SRC_PARQUET.rglob("*"):
        if src_file.is_dir():
            continue
        rel = src_file.relative_to(SRC_PARQUET)
        dst_file = DST_PARQUET / rel
        dst_file.parent.mkdir(parents=True, exist_ok=True)
        if dst_file.exists() and dst_file.stat().st_size == src_file.stat().st_size:
            continue
        shutil.copy2(src_file, dst_file)
        copied += 1
    print(f"copied {copied} parquet/json files into {DST_PARQUET}")
    return copied


def copy_proxies() -> bool:
    """Copy proxies.csv from the legacy location to the NM tree; return True on success."""
    if not SRC_PROXIES.exists():
        print(f"WARN: proxies.csv not found at {SRC_PROXIES} (already provisioned in NM tree)")
        return False
    DST_PROXIES.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(SRC_PROXIES, DST_PROXIES)
    print(f"copied {SRC_PROXIES} -> {DST_PROXIES}")
    return True


if __name__ == "__main__":
    copy_parquet_tree()
    copy_proxies()
