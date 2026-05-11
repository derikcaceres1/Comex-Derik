"""
Script para diagnosticar missing values no gold OM Brasil.

Verifica:
1. Quantos IDs têm NaN em Valor ou Valor_Cif
2. Se o problema é no último mês (trailing NaN) ou interno
3. Se é geral ou pontual (quais IDs afetados)

USO (a partir da pasta comex/):
    python OM/COMEX_BRA/diagnostico_missing_values.py
"""

import sys
import os
from pathlib import Path
import pandas as pd
import warnings
warnings.filterwarnings("ignore")

project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

# Caminhos possíveis do gold OM
GOLD_PATHS = [
    project_root / "OM" / "dados" / "BRA" / "gold",
    project_root / "staging" / "comex" / "BRA" / "gold",
]


def main():
    # Encontrar gold mais recente
    gold_df = None
    for gold_dir in GOLD_PATHS:
        if gold_dir.exists():
            files = sorted(gold_dir.glob("gold_*.parquet"))
            if files:
                gold_path = files[-1]
                print(f"Gold encontrado: {gold_path}")
                gold_df = pd.read_parquet(gold_path)
                break

    if gold_df is None:
        print("Gold não encontrado. Caminhos verificados:")
        for p in GOLD_PATHS:
            print(f"  {p}")
        return

    print(f"\nRegistros totais : {len(gold_df):,}")
    print(f"Colunas          : {list(gold_df.columns)}")

    gold_df['Data'] = pd.to_datetime(gold_df['Data'], errors='coerce')
    print(f"Período          : {gold_df['Data'].min().strftime('%Y-%m')} → {gold_df['Data'].max().strftime('%Y-%m')}")

    # ── Análise de NaNs ──────────────────────────────────────────────────────
    print(f"\n{'='*55}")
    print("  ANÁLISE DE MISSING VALUES")
    print(f"{'='*55}")

    for col in ['Valor', 'Valor_Cif']:
        if col not in gold_df.columns:
            continue
        total_nan = gold_df[col].isna().sum()
        pct = total_nan / len(gold_df) * 100
        ids_com_nan = gold_df[gold_df[col].isna()]['ID'].nunique() if 'ID' in gold_df.columns else '?'
        print(f"\n  {col}:")
        print(f"    NaNs totais     : {total_nan:,} ({pct:.1f}%)")
        print(f"    IDs afetados    : {ids_com_nan}")

    # ── Trailing NaN (último mês) ────────────────────────────────────────────
    print(f"\n{'='*55}")
    print("  TRAILING NaN (ÚLTIMO MÊS DE CADA ID)")
    print(f"{'='*55}")

    if 'ID' in gold_df.columns and 'Valor' in gold_df.columns:
        ultimo_por_id = gold_df.sort_values('Data').groupby('ID').last().reset_index()
        trailing_nan = ultimo_por_id[ultimo_por_id['Valor'].isna()]
        print(f"\n  IDs com NaN no último registro : {len(trailing_nan):,}")
        if not trailing_nan.empty:
            print(f"  Exemplo de IDs afetados:")
            print(trailing_nan[['ID', 'Data', 'Valor']].head(10).to_string(index=False))

    # ── Gaps internos ────────────────────────────────────────────────────────
    print(f"\n{'='*55}")
    print("  GAPS INTERNOS (MESES FALTANDO NA SEQUÊNCIA)")
    print(f"{'='*55}")

    if 'ID' in gold_df.columns:
        ids_com_gap = []
        for id_val, grupo in gold_df.groupby('ID'):
            meses = sorted(grupo['Data'].dt.to_period('M').unique())
            for i in range(1, len(meses)):
                if meses[i] != meses[i-1] + 1:
                    ids_com_gap.append(id_val)
                    break
        print(f"\n  IDs com gap interno na série : {len(ids_com_gap):,}")
        if ids_com_gap:
            print(f"  Primeiros IDs afetados: {ids_com_gap[:10]}")

    print(f"\n{'='*55}")
    if total_nan == 0:
        print("  ✓ Nenhum missing value detectado no gold!")
    elif pct < 1:
        print("  ⚠  Missing values pontuais (< 1%) — problema isolado")
    elif pct < 10:
        print("  ⚠  Missing values moderados (1-10%) — verificar IDs afetados")
    else:
        print("  ✗  Missing values generalizados (> 10%) — problema sistêmico")
    print(f"{'='*55}\n")


if __name__ == "__main__":
    os.chdir(project_root)
    main()
