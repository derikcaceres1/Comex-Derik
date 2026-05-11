"""
Executa o pipeline COMEX BRA NM completo com upload.
Fluxo: collect -> update_historical -> normalize_historical -> calculate -> upload
"""
import sys
import os
from pathlib import Path
from datetime import datetime
import pandas as pd

# Setup de paths
_this_dir     = Path(__file__).parent                            # .../COMEX_BRA
_serie_path   = _this_dir.parent                                 # .../serie_temporal
_nm_path      = _serie_path.parent                              # .../NM
_comex_root   = _nm_path.parent                                 # .../comex

# Garante que os módulos são encontrados
for p in [str(_comex_root), str(_nm_path), str(_serie_path), str(_this_dir)]:
    if p not in sys.path:
        sys.path.insert(0, p)

# CWD deve ser a raiz do projeto para que paths relativos (NM/dados/) funcionem
os.chdir(str(_comex_root))

from COMEX_BRA_NM import COMEX_BRA_NM

# Calcula start_date (5 meses atrás garante cobertura do ano anterior)
today = datetime.now()
start = (today.replace(day=1) - pd.DateOffset(months=5)).to_pydatetime()

print(f"Período de coleta: {start.strftime('%Y-%m-%d')} até hoje")
print(f"CWD: {os.getcwd()}")
print()

# Inicializa com developing=True (usa NM/dados/ local) + upload ativado
pipeline = COMEX_BRA_NM(start_date=start, use_azure=False, developing=True)

# Roda pipeline completo incluindo upload
result_df = pipeline.run()

print(f"\nPipeline concluído! Total de registros: {len(result_df)}")
