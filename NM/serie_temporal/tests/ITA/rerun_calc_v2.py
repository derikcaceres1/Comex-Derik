"""Re-roda o pipeline ITA v2 pulando collect/upload, usando o histórico já em disco.

Útil pra iterar em ajustes de filtros/cálculo sem refazer leitura do EUR e sem
subir nada na plataforma.
"""

import sys
from pathlib import Path

# Resolve o diretório COMEX_ITA/ a partir da localização deste arquivo em tests/ITA/
_ITA_DIR = Path(__file__).parent.parent.parent / "COMEX_ITA"
sys.path.insert(0, str(_ITA_DIR))

from COMEX_ITA_NM_v2 import COMEX_ITA_NM_v2  # noqa: E402


def main():
    pipeline = COMEX_ITA_NM_v2(start_date=None, developing=True)
    pipeline.run(skip_phases=["collect", "upload"])
    return pipeline


if __name__ == "__main__":
    p = main()
    print(
        f"\nRe-execução v2 ITA concluída.\n"
        f"  silver:    {len(p.silver_df) if p.silver_df is not None else 0}\n"
        f"  dropped:   {len(p.dropped_df) if p.dropped_df is not None else 0}\n"
        f"  gold v2:   {len(p.gold_df) if p.gold_df is not None else 0}"
    )
