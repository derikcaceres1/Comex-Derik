"""
Script de diagnóstico do historical.parquet do NM Brasil.

Execute este script a partir da raiz do projeto (pasta comex/) para verificar:
1. O que está no historical.parquet atual (datas min/max, tamanho)
2. O que está no cache do raw_df coletado (quais meses cobertos)
3. O que aconteceria se o merge fosse executado agora (simulação)
4. Se o guard de proteção estaria bloqueando a atualização
"""

import sys
import os
from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd
import warnings

warnings.filterwarnings("ignore")

# Adicionar paths necessários ao sys.path
script_dir = Path(__file__).parent.resolve()           # .../COMEX_BRA/
serie_temporal_path = script_dir.parent                # .../serie_temporal/
project_root = script_dir.parent.parent.parent         # .../comex/

for p in [str(script_dir), str(serie_temporal_path), str(project_root)]:
    if p not in sys.path:
        sys.path.insert(0, p)

# ─── Caminhos possíveis para o historical ────────────────────────────────────
POSSIBLE_HISTORICAL_PATHS = [
    Path("NM/dados/BRA/database/historical.parquet"),
    Path("NM/serie_temporal/COMEX_BRA/BRA/database/historical.parquet"),
]

# Caminhos possíveis para o cache raw
POSSIBLE_CACHE_PATHS = [
    Path("NM/dados/BRA/cache"),
]

# ─── Caminhos possíveis para os raw parquets ─────────────────────────────────
POSSIBLE_RAW_PATHS = [
    Path("NM/dados/BRA/raw"),
]


def find_file(paths):
    """Retorna o primeiro caminho existente da lista."""
    for p in paths:
        if p.exists():
            return p
    return None


def summarize_df(df: pd.DataFrame, label: str):
    """Exibe um resumo do DataFrame."""
    print(f"\n{'='*60}")
    print(f"  {label}")
    print(f"{'='*60}")
    print(f"  Registros     : {len(df):,}")
    print(f"  Colunas       : {list(df.columns)}")

    # Tentar identificar coluna de data
    date_col = None
    for col in ['Data', 'data']:
        if col in df.columns:
            date_col = col
            break

    if date_col is None and 'CO_ANO' in df.columns and 'CO_MES' in df.columns:
        df = df.copy()
        df['_Data'] = pd.to_datetime(
            df['CO_ANO'].astype(str) + '-' +
            df['CO_MES'].astype(str).str.zfill(2) + '-01'
        )
        date_col = '_Data'

    if date_col:
        try:
            datas = pd.to_datetime(df[date_col], errors='coerce').dropna()
            print(f"  Data mínima   : {datas.min().strftime('%Y-%m')}")
            print(f"  Data máxima   : {datas.max().strftime('%Y-%m')}")
            meses = datas.dt.to_period('M').unique()
            meses_sorted = sorted(meses)
            print(f"  Total meses   : {len(meses_sorted)}")
            # Detectar gaps
            gaps = []
            for i in range(1, len(meses_sorted)):
                prev = meses_sorted[i - 1]
                curr = meses_sorted[i]
                expected = prev + 1
                if curr != expected:
                    gaps.append(f"{expected} a {curr - 1}")
            if gaps:
                print(f"  ⚠️  GAPS DETECTADOS: {gaps}")
            else:
                print(f"  ✓  Sem gaps na sequência mensal")
        except Exception as e:
            print(f"  ⚠️  Erro ao analisar datas: {e}")
    else:
        print(f"  ⚠️  Coluna de data não encontrada. Colunas disponíveis: {list(df.columns)}")


def simulate_merge(historical_df: pd.DataFrame, raw_df: pd.DataFrame, update_months: int = 3):
    """Simula o que _update_historical_data faria com esses dados."""
    print(f"\n{'='*60}")
    print(f"  SIMULAÇÃO DO MERGE (update_months={update_months})")
    print(f"{'='*60}")

    hist = historical_df.copy()
    raw = raw_df.copy()

    # Criar coluna Data
    if 'Data' not in hist.columns and 'CO_ANO' in hist.columns:
        hist['Data'] = pd.to_datetime(
            hist['CO_ANO'].astype(str) + '-' +
            hist['CO_MES'].astype(str).str.zfill(2) + '-01'
        )
    elif 'Data' in hist.columns:
        hist['Data'] = pd.to_datetime(hist['Data'], errors='coerce')

    if 'Data' not in raw.columns and 'CO_ANO' in raw.columns:
        raw['Data'] = pd.to_datetime(
            raw['CO_ANO'].astype(str) + '-' +
            raw['CO_MES'].astype(str).str.zfill(2) + '-01'
        )
    elif 'Data' in raw.columns:
        raw['Data'] = pd.to_datetime(raw['Data'], errors='coerce')

    hist['Data_month'] = hist['Data'].dt.to_period('M').dt.to_timestamp()
    raw['Data_month'] = raw['Data'].dt.to_period('M').dt.to_timestamp()

    max_hist_date = hist['Data'].max()
    cutoff_date = (max_hist_date - pd.DateOffset(months=update_months)).replace(day=1)

    print(f"  Data máxima do histórico : {max_hist_date.strftime('%Y-%m')}")
    print(f"  Cutoff calculado         : {cutoff_date.strftime('%Y-%m')} (max − {update_months} meses)")

    historical_old = hist[hist['Data_month'] <= cutoff_date]
    new_data_filtered = raw[raw['Data_month'] > cutoff_date]

    print(f"\n  Histórico preservado (≤ {cutoff_date.strftime('%Y-%m')}) : {len(historical_old):,} registros")
    print(f"  Novos dados (> {cutoff_date.strftime('%Y-%m')})         : {len(new_data_filtered):,} registros")

    if not new_data_filtered.empty:
        raw_min = new_data_filtered['Data'].min().strftime('%Y-%m')
        raw_max = new_data_filtered['Data'].max().strftime('%Y-%m')
        print(f"  Range dos novos dados    : {raw_min} → {raw_max}")

    # Detectar meses que serão perdidos
    meses_dropados = hist[hist['Data_month'] > cutoff_date]['Data_month'].dt.to_period('M').unique()
    meses_raw = new_data_filtered['Data_month'].dt.to_period('M').unique() if not new_data_filtered.empty else []
    meses_dropados_sorted = sorted(meses_dropados)
    meses_raw_sorted = sorted(meses_raw)

    meses_faltando = set(meses_dropados_sorted) - set(meses_raw_sorted)
    if meses_faltando:
        print(f"\n  ⚠️  MESES REMOVIDOS DO HISTÓRICO SEM SUBSTITUTO:")
        for m in sorted(meses_faltando):
            print(f"      → {m} (será perdido!)")

    combined_size = len(historical_old) + len(new_data_filtered)
    existing_size = len(hist)

    print(f"\n  Tamanho existente : {existing_size:,}")
    print(f"  Tamanho combinado : {combined_size:,}")

    if combined_size < existing_size:
        print(f"\n  🔴 GUARD BLOQUEARIA A ATUALIZAÇÃO (combinado < existente)")
        print(f"     → historical_df ficará com dados até {max_hist_date.strftime('%Y-%m')}")
        print(f"     → Gold output será gerado até {max_hist_date.strftime('%Y-%m')}")
    elif combined_size == existing_size:
        print(f"\n  🔴 GUARD BLOQUEARIA A ATUALIZAÇÃO (combinado == existente)")
    else:
        print(f"\n  🟡 Guard passaria, mas há gaps nas datas!")
        if meses_faltando:
            print(f"     Meses sem dados: {sorted(meses_faltando)}")
            print(f"     Esses meses serão preenchidos por interpolação no calculate()")


def main():
    print("\n" + "="*60)
    print("  DIAGNÓSTICO NM BRASIL — HISTORICAL PARQUET")
    print("="*60)
    print(f"  Executado em: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # 1. Encontrar e carregar historical.parquet
    hist_path = find_file(POSSIBLE_HISTORICAL_PATHS)
    if hist_path is None:
        print("\n  ❌ historical.parquet NÃO encontrado nos caminhos:")
        for p in POSSIBLE_HISTORICAL_PATHS:
            print(f"     {p.absolute()}")
        print("\n  → Execute o script a partir da pasta raiz do projeto (onde fica a pasta 'comex/')")
        print("     ou ajuste POSSIBLE_HISTORICAL_PATHS acima para o caminho correto.")
        hist_df = None
    else:
        print(f"\n  ✓ historical.parquet encontrado: {hist_path.absolute()}")
        hist_df = pd.read_parquet(hist_path)
        summarize_df(hist_df, "HISTORICAL.PARQUET")

    # 2. Encontrar cache de raw data
    for cache_dir in POSSIBLE_CACHE_PATHS:
        if cache_dir.exists():
            cache_files = sorted(cache_dir.glob("import_cache_*.parquet"))
            if cache_files:
                latest_cache = cache_files[-1]
                print(f"\n  ✓ Cache de import encontrado: {latest_cache.name}")
                cache_df = pd.read_parquet(latest_cache)
                summarize_df(cache_df, f"CACHE IMPORT ({latest_cache.name})")
            else:
                print(f"\n  ℹ️  Nenhum cache de import encontrado em {cache_dir}")

    # 3. Encontrar raw parquets
    raw_df_combined = pd.DataFrame()
    for raw_dir in POSSIBLE_RAW_PATHS:
        if raw_dir.exists():
            raw_files = sorted(raw_dir.glob("import_raw_*.parquet"))
            if raw_files:
                latest_raw = raw_files[-1]
                print(f"\n  ✓ Raw import encontrado: {latest_raw.name}")
                raw_df_combined = pd.read_parquet(latest_raw)
                summarize_df(raw_df_combined, f"RAW IMPORT ({latest_raw.name})")

    # 4. Simular merge se tivermos ambos
    if hist_df is not None and not hist_df.empty and not raw_df_combined.empty:
        simulate_merge(hist_df, raw_df_combined, update_months=3)

    print(f"\n{'='*60}")
    print("  CONCLUSÃO E PRÓXIMOS PASSOS")
    print(f"{'='*60}")
    if hist_df is not None:
        print("""
  Se o diagnóstico mostrou:
  ──────────────────────────────────────────────────────
  1. Historical até Nov/2025
  2. Raw import apenas com dados de Jan/2026+
  3. Guard bloqueando ou gap em Dez/2025
  ──────────────────────────────────────────────────────
  → Execute o script 'corrigir_historical.py' ao lado
    para corrigir baixando também os dados de 2025.
""")
    print("="*60)


if __name__ == "__main__":
    # Mudar para a raiz do projeto para que os caminhos relativos funcionem
    project_candidates = [
        Path(__file__).parent.parent.parent.parent,  # comex 1/comex/ → comex 1/
        Path(__file__).parent.parent.parent,          # NM/serie_temporal/COMEX_BRA/ → NM/../..
    ]
    for candidate in project_candidates:
        comex_dir = candidate / "comex" if (candidate / "comex").exists() else candidate
        if (comex_dir / "NM").exists():
            os.chdir(comex_dir)
            print(f"  Diretório de trabalho: {Path.cwd()}")
            break

    main()
