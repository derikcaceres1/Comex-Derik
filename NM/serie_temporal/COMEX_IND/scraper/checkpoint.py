import json
import os
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from .constants import EXPECTED_COLUMNS, PARQUET_BASE, PARQUET_COMPRESSION, PARQUET_ENGINE


def parquet_path(year: int, month: int) -> Path:
    return PARQUET_BASE / f"year={year}" / f"month={month:02d}" / f"meidb_{year}_{month:02d}.parquet"


def is_month_complete(year: int, month: int) -> bool:
    path = parquet_path(year, month)
    if not path.exists() or path.stat().st_size == 0:
        return False
    try:
        df = pd.read_parquet(path, columns=EXPECTED_COLUMNS)
        return len(df) > 0
    except Exception:
        return False


def write_parquet(df: pd.DataFrame, year: int, month: int) -> None:
    path = parquet_path(year, month)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp.parquet")
    df.to_parquet(tmp, engine=PARQUET_ENGINE, compression=PARQUET_COMPRESSION, index=False)
    os.replace(tmp, path)


def write_checkpoint_json(
    year: int,
    month: int,
    df: pd.DataFrame,
    failed_country_ids: list[int],
) -> None:
    path = parquet_path(year, month).with_suffix(".json")
    meta = {
        "year": year,
        "month": month,
        "row_count": len(df),
        "country_count": int(df["country_id"].nunique()) if "country_id" in df.columns else 0,
        "hs_code_count": int(df["hs_code"].nunique()) if "hs_code" in df.columns else 0,
        "failed_country_ids": sorted(set(failed_country_ids)),
        "written_at": datetime.now(timezone.utc).isoformat(),
    }
    path.write_text(json.dumps(meta, indent=2), encoding="utf-8")
