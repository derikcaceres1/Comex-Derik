"""Compara o gold do BRA gerado pelo v2 com o gold mais recente do v1.

Uso:
    python comparar_gold_v1_v2.py

Requisitos:
    • Pelo menos um arquivo gold_NM_*.parquet (v1) em NM/dados/BRA/gold/
    • Pelo menos um arquivo gold_NM_v2_*.parquet (v2) em NM/dados/BRA/gold/

O script imprime:
    1. Quantidade de IDs e linhas em cada gold
    2. IDs que estão APENAS no v1 (e o motivo, lendo dropped_*.parquet do v2)
    3. IDs que estão APENAS no v2 (raro)
    4. Para os IDs em comum: diferenças de Valor e Valor_Cif por (ID, Data)
    5. Resumo das maiores divergências

Não modifica nenhum arquivo. Não sobe nada na API.
"""

from __future__ import annotations

from pathlib import Path
import sys

import pandas as pd

# tests/BRA/ → tests/ → serie_temporal/ → NM/
_NM_DIR = Path(__file__).parent.parent.parent.parent
GOLD_DIR = _NM_DIR / "dados" / "BRA" / "gold"
SILVER_DIR = _NM_DIR / "dados" / "BRA" / "silver"


def _latest(pattern: str, directory: Path, exclude_substr: str = None) -> Path | None:
    files = list(directory.glob(pattern))
    if exclude_substr:
        files = [f for f in files if exclude_substr not in f.name]
    files = sorted(files, reverse=True)
    return files[0] if files else None


def _load_gold_v1() -> tuple[Path | None, pd.DataFrame]:
    path = _latest("gold_NM_*.parquet", GOLD_DIR, exclude_substr="_v2_")
    if path is None:
        return None, pd.DataFrame()
    df = pd.read_parquet(path)
    df["Data"] = pd.to_datetime(df["Data"])
    return path, df


def _load_gold_v2() -> tuple[Path | None, pd.DataFrame]:
    path = _latest("gold_NM_v2_*.parquet", GOLD_DIR)
    if path is None:
        return None, pd.DataFrame()
    df = pd.read_parquet(path)
    df["Data"] = pd.to_datetime(df["Data"])
    return path, df


def _load_dropped() -> pd.DataFrame:
    path = _latest("dropped_*.parquet", SILVER_DIR)
    if path is None:
        return pd.DataFrame(columns=["IDIndicePrincipal", "reason", "detail"])
    return pd.read_parquet(path)


def main():
    if not GOLD_DIR.exists():
        print(f"Diretório de gold não encontrado: {GOLD_DIR}")
        sys.exit(1)

    v1_path, v1 = _load_gold_v1()
    v2_path, v2 = _load_gold_v2()

    if v1.empty:
        print("Gold v1 não encontrado — rode o pipeline antigo primeiro.")
        sys.exit(1)
    if v2.empty:
        print("Gold v2 não encontrado — rode `python COMEX_BRA_NM_v2.py` primeiro.")
        sys.exit(1)

    if "ID" not in v1.columns and "IDIndicePrincipal" in v1.columns:
        v1 = v1.rename(columns={"IDIndicePrincipal": "ID"})

    print(f"Gold v1: {v1_path.name}  ->  {len(v1)} linhas, {v1['ID'].nunique()} IDs")
    print(f"Gold v2: {v2_path.name}  ->  {len(v2)} linhas, {v2['ID'].nunique()} IDs")
    print()

    ids_v1 = set(v1["ID"].unique())
    ids_v2 = set(v2["ID"].unique())
    only_v1 = ids_v1 - ids_v2
    only_v2 = ids_v2 - ids_v1
    common = ids_v1 & ids_v2

    print("=" * 70)
    print("DIFERENÇA DE COBERTURA")
    print("=" * 70)
    print(f"IDs só no v1:  {len(only_v1):>5}  (saíram pelos filtros explícitos do v2)")
    print(f"IDs só no v2:  {len(only_v2):>5}")
    print(f"IDs em comum:  {len(common):>5}")
    print()

    if only_v1:
        dropped = _load_dropped()
        if not dropped.empty:
            sub = dropped[dropped["IDIndicePrincipal"].isin(only_v1)]
            counts = sub.groupby("reason").size().sort_values(ascending=False)
            print("Por que o v2 descartou esses IDs:")
            for reason, n in counts.items():
                print(f"  {reason:<25} -> {n} ID(s)")
            print()
            sample = sub.head(10)[["IDIndicePrincipal", "reason", "detail"]]
            if not sample.empty:
                print("Amostra (até 10):")
                print(sample.to_string(index=False))
                print()

    if not common:
        print("Nenhum ID em comum — fim da comparação.")
        return

    keep = ["ID", "Data", "Valor", "Valor_Cif"]
    a = v1[keep].rename(columns={"Valor": "Valor_v1", "Valor_Cif": "Valor_Cif_v1"})
    b = v2[keep].rename(columns={"Valor": "Valor_v2", "Valor_Cif": "Valor_Cif_v2"})
    merged = a.merge(b, on=["ID", "Data"], how="outer", indicator=True)

    n_only_left = (merged["_merge"] == "left_only").sum()
    n_only_right = (merged["_merge"] == "right_only").sum()
    n_both = (merged["_merge"] == "both").sum()

    print("=" * 70)
    print("DIFERENÇA POR (ID, Data) — apenas IDs em comum")
    print("=" * 70)
    print(f"Pontos só no v1: {n_only_left}")
    print(f"Pontos só no v2: {n_only_right}")
    print(f"Pontos em ambos: {n_both}")
    print()

    both = merged[merged["_merge"] == "both"].copy()

    def _summary(col_v1: str, col_v2: str, label: str):
        v1_vals = both[col_v1]
        v2_vals = both[col_v2]
        nan_v1, nan_v2 = v1_vals.isna(), v2_vals.isna()
        both_nan = nan_v1 & nan_v2
        both_filled = ~nan_v1 & ~nan_v2
        diff = (v2_vals - v1_vals).abs()
        eq = (diff < 1e-6) | both_nan
        diff_filled = diff[both_filled]
        rel = (diff_filled / v1_vals[both_filled].replace(0, pd.NA)).abs()
        total = len(both)
        print(f"\n{label}:")
        print(f"  iguais (incl. NaN==NaN):       {eq.sum():>7}/{total} ({eq.sum() / total:.1%})")
        print(f"  ambos NaN:                     {both_nan.sum():>7}")
        print(f"  NaN apenas no v1:              {(nan_v1 & ~nan_v2).sum():>7}")
        print(f"  NaN apenas no v2:              {(~nan_v1 & nan_v2).sum():>7}")
        print(f"  ambos com valor (comparáveis): {both_filled.sum():>7}")
        if both_filled.sum() > 0:
            iguais_filled = (diff_filled < 1e-6).sum()
            print(f"  desses, iguais:                {iguais_filled:>7}/{both_filled.sum()}"
                  f" ({iguais_filled / both_filled.sum():.1%})")
            print(f"  diff média (absoluta):         {diff_filled.mean():.4f}")
            print(f"  diff relativa média:           {rel.mean() * 100:.2f}%")
            print(f"  diff relativa mediana:         {rel.median() * 100:.2f}%")

    _summary("Valor_v1", "Valor_v2", "Valor (FOB/peso)")
    _summary("Valor_Cif_v1", "Valor_Cif_v2", "Valor_Cif (CIF/peso)")
    print()

    top = both.assign(diff_valor=(both["Valor_v2"] - both["Valor_v1"]).abs()).sort_values(
        "diff_valor", ascending=False
    ).head(10)
    print("Top 10 maiores divergências em Valor:")
    print(top[["ID", "Data", "Valor_v1", "Valor_v2", "diff_valor"]].to_string(index=False))


if __name__ == "__main__":
    main()
