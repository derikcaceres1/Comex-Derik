"""
Script para corrigir o historical.parquet do NM Brasil.

PROBLEMA:
  - O historical.parquet tem dados até Nov/2025
  - O pipeline com start_date padrão (hoje - 60 dias) só baixa arquivos de 2026
  - Dezembro/2025 nunca foi coletado → GAP na série
  - O guard de proteção pode bloquear o merge ou o gap quebra os cálculos
  
SOLUÇÃO:
  - Forçar start_date = 2025-10-01 para baixar o arquivo anual de 2025 + 2026
  - Isso garante que Dez/2025 seja coletado e o merge funcione corretamente
  - O historical será atualizado com dados até Fev/2026 (ou mais recente disponível)

USO:
  python corrigir_historical.py
  
  Ou com data personalizada:
  python corrigir_historical.py --start 2025-10-01
"""

import sys
import argparse
from datetime import datetime
from pathlib import Path

# Adicionar paths necessários ao sys.path
script_dir = Path(__file__).parent.resolve()           # .../COMEX_BRA/
serie_temporal_path = script_dir.parent                # .../serie_temporal/
project_root = script_dir.parent.parent.parent         # .../comex/

# script_dir     → permite "from COMEX_BRA_NM import ..."
# serie_temporal_path → permite "from costdrivers_comex_NM import ..."
# project_root   → permite "from library.costdrivers import ..."
for p in [str(script_dir), str(serie_temporal_path), str(project_root)]:
    if p not in sys.path:
        sys.path.insert(0, p)

from COMEX_BRA_NM import COMEX_BRA_NM


def main():
    parser = argparse.ArgumentParser(description="Corrige o historical.parquet do NM Brasil")
    parser.add_argument(
        "--start",
        type=str,
        default="2025-10-01",
        help="Data inicial para coleta (formato YYYY-MM-DD). Padrão: 2025-10-01 (baixa 2025+2026)"
    )
    parser.add_argument(
        "--azure",
        action="store_true",
        default=False,
        help="Usar Azure Storage (padrão: False, usa storage local)"
    )
    args = parser.parse_args()

    start_date = datetime.strptime(args.start, "%Y-%m-%d")
    
    print(f"""
{'='*65}
  CORREÇÃO DO HISTORICAL.PARQUET — NM BRASIL
{'='*65}
  Start date   : {start_date.strftime('%Y-%m-%d')}
  Anos coletados: {start_date.year} até {datetime.now().year}
  Storage       : {'Azure' if args.azure else 'Local (developing=True)'}
  
  O que este script faz:
  ─────────────────────────────────────────────────────────────
  1. Baixa arquivos CSV de {start_date.year} E {datetime.now().year} do portal gov.br
     → Isso garante que Dez/{start_date.year} seja coletado
  2. Executa update_historical() com os dados completos
     → Merge: histórico até Ago/2025 + Set/2025 a Fev/2026
  3. Executa calculate() com histórico atualizado
     → Gold output esperado: até Fev/2026
  ─────────────────────────────────────────────────────────────
  ⚠️  O cache atual (import_cache_20260101.parquet) será IGNORADO
     pois estamos usando uma data de início diferente.
{'='*65}
""")

    confirm = input("  Deseja continuar? (s/N): ").strip().lower()
    if confirm != 's':
        print("  Operação cancelada.")
        return

    print(f"\n  Iniciando pipeline com start_date={start_date.strftime('%Y-%m-%d')}...\n")

    pipeline = COMEX_BRA_NM(
        start_date=start_date,
        developing=not args.azure,
        use_azure=args.azure
    )

    result = pipeline.run()

    if result is not None and not result.empty:
        # Mostrar range de datas do resultado
        if 'Data' in result.columns:
            data_col = 'Data'
        else:
            data_col = None
            
        print(f"\n{'='*65}")
        print(f"  RESULTADO FINAL")
        print(f"{'='*65}")
        print(f"  Registros no gold output : {len(result):,}")
        if data_col:
            import pandas as pd
            datas = pd.to_datetime(result[data_col], errors='coerce').dropna()
            print(f"  Data mínima do gold      : {datas.min().strftime('%Y-%m')}")
            print(f"  Data máxima do gold      : {datas.max().strftime('%Y-%m')}")
        print(f"{'='*65}\n")
    else:
        print("\n  ⚠️  Pipeline retornou DataFrame vazio ou None.")


if __name__ == "__main__":
    main()
