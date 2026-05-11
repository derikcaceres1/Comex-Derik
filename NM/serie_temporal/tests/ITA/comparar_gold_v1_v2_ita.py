"""Compara gold_NM_*.parquet (v1) com gold_NM_v2_*.parquet (v2) para ITA."""
import sys
from pathlib import Path
import numpy as np
import pandas as pd

# tests/ITA/ → tests/ → serie_temporal/ → NM/ → comex/
_COMEX_DIR = Path(__file__).parent.parent.parent.parent.parent
BASE   = _COMEX_DIR / "NM" / "dados" / "ITA" / "gold"
SILVER = _COMEX_DIR / "NM" / "dados" / "ITA" / "silver"


def _latest(pattern: str, directory: Path, exclude: str = "") -> Path | None:
    files = [f for f in directory.glob(pattern) if not (exclude and exclude in f.name)]
    if not files:
        return None
    return sorted(files, key=lambda f: f.stat().st_mtime)[-1]


def main():
    v1_path = _latest("gold_NM_*.parquet", BASE, exclude="_v2_")
    v2_path = _latest("gold_NM_v2_*.parquet", BASE)
    dropped_path = _latest("dropped_*.parquet", SILVER)

    if not v1_path or not v2_path:
        print("Arquivos gold v1 ou v2 não encontrados.")
        return

    v1 = pd.read_parquet(v1_path)
    v2 = pd.read_parquet(v2_path)
    dropped = pd.read_parquet(dropped_path) if dropped_path else pd.DataFrame()

    print(f"Gold v1: {v1_path.name}  ->  {len(v1)} linhas, {v1['ID'].nunique()} IDs")
    print(f"Gold v2: {v2_path.name}  ->  {len(v2)} linhas, {v2['ID'].nunique()} IDs")

    ids_v1 = set(v1["ID"].unique())
    ids_v2 = set(v2["ID"].unique())
    only_v1 = ids_v1 - ids_v2
    only_v2 = ids_v2 - ids_v1
    common = ids_v1 & ids_v2

    print(f"\n{'='*70}")
    print("DIFERENÇA DE COBERTURA")
    print(f"{'='*70}")
    print(f"IDs só no v1:   {len(only_v1)}  (saíram pelos filtros explícitos do v2)")
    print(f"IDs só no v2:   {len(only_v2)}")
    print(f"IDs em comum:   {len(common)}")

    if not dropped.empty and len(only_v1) > 0:
        sub = dropped[dropped["IDIndicePrincipal"].isin(only_v1)]
        if not sub.empty:
            print("\nPor que o v2 descartou esses IDs:")
            for reason, cnt in sub.groupby("reason")["IDIndicePrincipal"].nunique().items():
                print(f"  {reason:<30} -> {cnt} ID(s)")
            print("\nAmostra (até 10):")
            print(sub[["IDIndicePrincipal", "reason", "detail"]]
                  .drop_duplicates("IDIndicePrincipal").head(10).to_string(index=False))

    print(f"\n{'='*70}")
    print("DIFERENÇA POR (ID, Data) — apenas IDs em comum")
    print(f"{'='*70}")

    v1c = v1[v1["ID"].isin(common)][["ID", "Data", "Valor", "Valor_Cif"]].copy()
    v2c = v2[v2["ID"].isin(common)][["ID", "Data", "Valor", "Valor_Cif"]].copy()
    v1c["Data"] = pd.to_datetime(v1c["Data"])
    v2c["Data"] = pd.to_datetime(v2c["Data"])

    only_in_v1 = len(v1c.merge(v2c[["ID", "Data"]], on=["ID", "Data"],
                                how="left", indicator=True).query("_merge=='left_only'"))
    only_in_v2 = len(v2c.merge(v1c[["ID", "Data"]], on=["ID", "Data"],
                                how="left", indicator=True).query("_merge=='left_only'"))
    merged = v1c.merge(v2c, on=["ID", "Data"], suffixes=("_v1", "_v2"))

    print(f"Pontos só no v1: {only_in_v1}")
    print(f"Pontos só no v2: {only_in_v2}")
    print(f"Pontos em ambos: {len(merged)}")

    for col in ("Valor", "Valor_Cif"):
        c1, c2 = f"{col}_v1", f"{col}_v2"
        if c1 not in merged or c2 not in merged:
            continue
        a = merged[c1].to_numpy(dtype=float)
        b = merged[c2].to_numpy(dtype=float)
        nan_eq = np.isnan(a) & np.isnan(b)
        close = np.isclose(a, b, rtol=1e-5, equal_nan=False)
        equal = nan_eq | close
        both_nan = int(nan_eq.sum())
        nan_only_v1 = int((np.isnan(a) & ~np.isnan(b)).sum())
        nan_only_v2 = int((~np.isnan(a) & np.isnan(b)).sum())
        both_filled = int((~np.isnan(a) & ~np.isnan(b)).sum())
        eq_filled = int((close & ~np.isnan(a) & ~np.isnan(b)).sum())
        diff = np.abs(a - b)
        rel = diff / np.where(np.abs(a) > 1e-9, np.abs(a), np.nan)

        print(f"\n{col} (FOB/peso):")
        print(f"  iguais (incl. NaN==NaN):        {equal.sum()}/{len(merged)} ({equal.mean()*100:.1f}%)")
        print(f"  ambos NaN:                       {both_nan:>6}")
        print(f"  NaN apenas no v1:                {nan_only_v1:>6}")
        print(f"  NaN apenas no v2:                {nan_only_v2:>6}")
        print(f"  ambos com valor (comparáveis):   {both_filled:>6}")
        if both_filled > 0:
            print(f"  desses, iguais:                 {eq_filled}/{both_filled} ({eq_filled/both_filled*100:.1f}%)")
            print(f"  diff média (absoluta):         {np.nanmean(diff):.4f}")
            print(f"  diff relativa média:           {np.nanmean(rel)*100:.2f}%")
            print(f"  diff relativa mediana:         {np.nanmedian(rel)*100:.2f}%")

    if len(merged) > 0:
        merged["diff_valor"] = (merged["Valor_v1"] - merged["Valor_v2"]).abs()
        top = merged.nlargest(10, "diff_valor")[["ID", "Data", "Valor_v1", "Valor_v2", "diff_valor"]]
        print(f"\nTop 10 maiores divergências em Valor:")
        print(top.to_string(index=False))


if __name__ == "__main__":
    main()
