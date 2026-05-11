"""
Executa o upload do gold validado via pipeline OM BRA oficial.
- Salva gold no Azure Blob Storage (staging/comex/BRA/gold/)
- Faz upload dos dados para a API costdrivers
"""
import sys
import pathlib
import pandas as pd
from datetime import datetime

_comex_root = pathlib.Path(__file__).parent.parent.parent   # .../comex
_om_root    = _comex_root / "OM"

if str(_comex_root) not in sys.path:
    sys.path.insert(0, str(_comex_root))
if str(_om_root) not in sys.path:
    sys.path.insert(0, str(_om_root))

from COMEX_BRA import COMEX_BRA

# --- Carrega gold validado ---
gold_path = _om_root / "dados" / "BRA" / "gold" / "gold_2026-04-14.parquet"
gold_df = pd.read_parquet(gold_path)

print(f"Gold carregado: {gold_df.shape} | {gold_df['Data'].min()} -> {gold_df['Data'].max()}")
print(f"IDs únicos: {gold_df['ID'].nunique()}")
print(f"Colunas: {list(gold_df.columns)}")
print()

# --- Inicializa pipeline com Azure ativado ---
pipeline = COMEX_BRA(use_azure=True)
pipeline.gold_df = gold_df

# --- Salva gold no blob (staging/comex/BRA/gold/) ---
date_str = datetime.now().strftime("%Y-%m-%d")
print(f"Salvando gold no Azure Blob: {pipeline.gold_path}/gold_{date_str}.parquet ...")
pipeline._save_to_storage(gold_df, pipeline.gold_path, f"gold_{date_str}.parquet")
print("Gold salvo no blob com sucesso.")
print()

# --- Upload para a API costdrivers ---
print("Iniciando upload para a API...")
pipeline.upload()
