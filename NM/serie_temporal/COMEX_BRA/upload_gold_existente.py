"""
Script para fazer upload do gold NM Brasil já gerado.

O gold_NM_2026-03-18.parquet foi gerado e validado pelo cliente (dados até Fev/2026).
Este script carrega esse gold e faz apenas o upload para a API, sem reprocessar nada.

USO (a partir da pasta comex/):
    python -m NM.serie_temporal.COMEX_BRA.upload_gold_existente
"""

import sys
from pathlib import Path
import pandas as pd
from glob import glob

# Paths
script_dir = Path(__file__).parent.resolve()
serie_temporal_path = script_dir.parent
project_root = script_dir.parent.parent.parent

for p in [str(script_dir), str(serie_temporal_path), str(project_root), str(project_root / "library")]:
    if p not in sys.path:
        sys.path.insert(0, p)

from COMEX_BRA_NM import COMEX_BRA_NM


def main():
    gold_dir = project_root / "NM" / "dados" / "BRA" / "gold"

    # Encontrar o gold mais recente
    gold_files = sorted(gold_dir.glob("gold_NM_*.parquet"))
    if not gold_files:
        print(f"Nenhum gold encontrado em: {gold_dir}")
        return

    gold_path = gold_files[-1]
    print(f"\nGold encontrado: {gold_path.name}")

    gold_df = pd.read_parquet(gold_path)
    print(f"Registros: {len(gold_df):,}")

    if 'Data' in gold_df.columns:
        datas = pd.to_datetime(gold_df['Data'], errors='coerce').dropna()
        print(f"Período: {datas.min().strftime('%Y-%m')} → {datas.max().strftime('%Y-%m')}")

    confirm = input("\nDeseja fazer upload desse gold para a API? (s/N): ").strip().lower()
    if confirm != 's':
        print("Operação cancelada.")
        return

    # Inicializar pipeline só para usar o método upload()
    pipeline = COMEX_BRA_NM(developing=True)

    # Injetar o gold já gerado diretamente no pipeline
    pipeline.gold_df = gold_df

    print("\nIniciando upload...")
    resultado = pipeline.upload()

    if resultado:
        print("\n✓ Upload concluído com sucesso!")
    else:
        print("\n✗ Upload falhou. Verifique os logs acima.")


if __name__ == "__main__":
    main()
