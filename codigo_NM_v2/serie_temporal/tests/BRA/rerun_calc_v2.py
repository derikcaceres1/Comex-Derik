"""Re-roda o pipeline BRA v2 pulando collect/upload, usando o histórico já em disco.

Útil pra iterar em ajustes de filtros/cálculo sem refazer download (1 min) e
sem subir nada na plataforma. Reaproveita o cache local de import/export e o
historical.parquet já existente.
"""

from datetime import datetime
import sys
from pathlib import Path

import pandas as pd

# Resolve o diretório COMEX_BRA/ a partir da localização deste arquivo em tests/BRA/
_BRA_DIR = Path(__file__).parent.parent.parent / "COMEX_BRA"
sys.path.insert(0, str(_BRA_DIR))

from COMEX_BRA_NM_v2 import COMEX_BRA_NM_v2  # noqa: E402


def main():
    today = datetime.now()
    start = (today.replace(day=1) - pd.DateOffset(months=5)).to_pydatetime()
    pipeline = COMEX_BRA_NM_v2(start_date=start, developing=True)

    # Pula collect (download do portal) e upload (não sobe nada).
    pipeline.run(skip_phases=["collect", "upload"])
    return pipeline


if __name__ == "__main__":
    p = main()
    print(
        f"\nRe-execução v2 BRA concluída.\n"
        f"  silver:    {len(p.silver_df) if p.silver_df is not None else 0}\n"
        f"  dropped:   {len(p.dropped_df) if p.dropped_df is not None else 0}\n"
        f"  gold v2:   {len(p.gold_df) if p.gold_df is not None else 0}"
    )
