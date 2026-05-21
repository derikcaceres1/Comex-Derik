"""Reads every scraped monthly parquet for India, normalises it through the pipeline
and writes the consolidated ``historical.parquet`` used by the NM v2 pipeline.

Run once from the GEP root:
    python Comex-Derik/NM/serie_temporal/COMEX_IND/bootstrap_historical.py
"""

import os
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd
import yaml

# costdrivers.py validates credentials at import time even in developing mode.
# Set dummy values so the module loads; they are never used in bootstrap.
os.environ.setdefault("COSTDRIVERS_PASSWORD", "bootstrap-not-used")
os.environ.setdefault("COSTDRIVERS_ENDPOINT", "https://api-costdrivers.gep.com/costdrivers-api")

_THIS = Path(__file__).resolve()
_PIPELINE_ROOT = _THIS.parent
_SERIE_TEMPORAL = _PIPELINE_ROOT.parent
_NM_ROOT = _SERIE_TEMPORAL.parent
_PROJECT_ROOT = _NM_ROOT.parent

for _p in (_SERIE_TEMPORAL, _PIPELINE_ROOT.parent):
    if str(_p) not in sys.path:
        sys.path.insert(0, str(_p))

from COMEX_IND.COMEX_IND_NM_v2 import COMEX_IND_NM_v2


SCRAPER_PARQUET_DIR = _NM_ROOT / "dados" / "IND" / "scraper_parquet"
HISTORICAL_OUT = _NM_ROOT / "dados" / "IND" / "database" / "historical.parquet"
DATA_CONTRACT_PATH = _SERIE_TEMPORAL / "data-contract.yaml"


def _list_month_dirs(base: Path):
    """Yield (year, month, path) for every parquet file found under a year/month partition tree."""
    months = []
    for year_dir in sorted(base.glob("year=*")):
        try:
            year = int(year_dir.name.split("=", 1)[1])
        except (ValueError, IndexError):
            continue
        for month_dir in sorted(year_dir.glob("month=*")):
            try:
                month = int(month_dir.name.split("=", 1)[1])
            except (ValueError, IndexError):
                continue
            for pq_file in sorted(month_dir.glob("*.parquet")):
                months.append((year, month, pq_file))
    return months


def _load_contract() -> dict:
    """Load and return the parsed data-contract.yaml for this pipeline."""
    if not DATA_CONTRACT_PATH.exists():
        raise FileNotFoundError(f"data-contract.yaml não encontrado em {DATA_CONTRACT_PATH}")
    with DATA_CONTRACT_PATH.open("r", encoding="utf-8") as fh:
        return yaml.safe_load(fh)


def main() -> int:
    """One-time migration: normalize all scraped months and write consolidated historical.parquet."""
    if not SCRAPER_PARQUET_DIR.exists():
        print(f"ERROR: scraper_parquet dir not found: {SCRAPER_PARQUET_DIR}", file=sys.stderr)
        return 1

    files = _list_month_dirs(SCRAPER_PARQUET_DIR)
    if not files:
        print(f"ERROR: nenhum parquet em {SCRAPER_PARQUET_DIR}", file=sys.stderr)
        return 1

    print(f"Encontrados {len(files)} arquivos parquet em {SCRAPER_PARQUET_DIR}")

    contract = _load_contract()
    pipeline = COMEX_IND_NM_v2(start_date=datetime(2000, 1, 1), developing=True, use_azure=False)

    import_frames = []
    export_frames = []

    for year, month, pq_file in files:
        df_month = pd.read_parquet(pq_file)
        if df_month.empty:
            continue
        for flow_label, holder in (("IMPORT", import_frames), ("EXPORT", export_frames)):
            df_flow = df_month[df_month["trade_flow"].astype(str) == flow_label]
            if df_flow.empty:
                continue
            holder.append(df_flow.copy())

    parts = []
    for flow_label, frames, import_export in (
        ("IMPORT", import_frames, 1),
        ("EXPORT", export_frames, 0),
    ):
        if not frames:
            print(f"WARN: nenhum dado de {flow_label} encontrado")
            continue
        raw = pd.concat(frames, ignore_index=True)
        normalized = pipeline.normalize_columns(raw, contract, import_export)
        normalized = pipeline._country_specific_treatment(normalized)
        if normalized is None or normalized.empty:
            print(f"WARN: normalização produziu DataFrame vazio para {flow_label}")
            continue
        parts.append(normalized)
        print(f"  {flow_label}: {len(normalized)} linhas normalizadas")

    if not parts:
        print("ERROR: nenhum dado normalizado disponível", file=sys.stderr)
        return 1

    historical = pd.concat(parts, ignore_index=True)
    HISTORICAL_OUT.parent.mkdir(parents=True, exist_ok=True)
    historical.to_parquet(HISTORICAL_OUT, engine="pyarrow", compression="snappy", index=False)

    n_rows = len(historical)
    n_dates = historical["Data"].nunique() if "Data" in historical.columns else 0
    date_min = historical["Data"].min() if "Data" in historical.columns and n_dates > 0 else None
    date_max = historical["Data"].max() if "Data" in historical.columns and n_dates > 0 else None
    null_per_col = historical.isnull().sum().to_dict()

    print("\n=== VALIDAÇÃO DO HISTÓRICO ===")
    print(f"Arquivo:   {HISTORICAL_OUT}")
    print(f"Linhas:    {n_rows}")
    print(f"Meses:     {n_dates}")
    print(f"Período:   {date_min} → {date_max}")
    print(f"Colunas:   {list(historical.columns)}")
    print("Nulos por coluna:")
    for col, n_null in null_per_col.items():
        print(f"  {col:15s}: {n_null}")
    if "ImportExport" in historical.columns:
        ie_counts = historical["ImportExport"].value_counts().to_dict()
        print(f"ImportExport: {ie_counts}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
