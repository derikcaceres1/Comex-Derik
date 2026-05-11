"""
Filtros do pipeline COMEX NM v2.

Cada filtro é uma função pura com o mesmo contrato:

    df_valid, df_dropped = filter_xxx(df, ...)

- df_valid: subset que passou no filtro (mesmas colunas do df de entrada,
  exceto quando o filtro também transforma o shape — F4 agrega).
- df_dropped: DataFrame com colunas DROPPED_COLUMNS, contendo um registro
  por entidade descartada (NCM, ID ou par país×data) e o motivo.

A ordem recomendada de aplicação está documentada em
ComexPipelineNMv2.filter_data().
"""

from typing import Tuple

import numpy as np
import pandas as pd

from nm_reasons import DropReason, DROPPED_COLUMNS

ID_COL = "IDIndicePrincipal"


def empty_dropped() -> pd.DataFrame:
    """DataFrame de descarte vazio com o schema esperado."""
    return pd.DataFrame(columns=DROPPED_COLUMNS)


def _drop_row(id_: object, ncm: object, ie: object, reason: DropReason, detail: str) -> dict:
    return {
        "IDIndicePrincipal": id_,
        "ncm": ncm,
        "ImportExport": ie,
        "reason": reason.value,
        "detail": detail,
    }


# =====================================================================
# F5 — linhas inválidas (peso/valor)
# =====================================================================
def filter_invalid_rows(
    df: pd.DataFrame,
    weight_col: str = "peso",
    value_col: str = "valor",
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Remove linhas com peso ou valor ausente / não positivo."""
    if df.empty:
        return df.copy(), empty_dropped()

    weight = pd.to_numeric(df[weight_col], errors="coerce")
    value = pd.to_numeric(df[value_col], errors="coerce")
    invalid = weight.le(0) | weight.isna() | value.le(0) | value.isna()

    df_valid = df.loc[~invalid].copy()
    if not invalid.any():
        return df_valid, empty_dropped()

    summary = (
        df.loc[invalid, ["ncm", "ImportExport"]]
        .drop_duplicates()
        .assign(
            IDIndicePrincipal=pd.NA,
            reason=DropReason.INVALID_ROW.value,
            detail=f"{int(invalid.sum())} linha(s) descartada(s) por peso ou valor não positivo",
        )
    )
    return df_valid, summary[DROPPED_COLUMNS]


# =====================================================================
# F1 — NCM mapeado em IDS_comex
# =====================================================================
def filter_ncm_mapped(
    df: pd.DataFrame,
    ids_table: pd.DataFrame,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Mantém apenas registros cujo (ncm, ImportExport) existe em IDS_comex.

    Anexa a coluna IDIndicePrincipal ao DataFrame retornado.
    """
    if df.empty:
        return df.copy(), empty_dropped()

    ids = ids_table[["NCM", "ImportExport", "IDIndicePrincipal"]].copy()
    ids["NCM"] = pd.to_numeric(ids["NCM"], errors="coerce").astype("Int64")
    ids["ImportExport"] = pd.to_numeric(ids["ImportExport"], errors="coerce").astype("Int64")

    work = df.copy()
    work["ncm"] = pd.to_numeric(work["ncm"], errors="coerce").astype("Int64")
    work["ImportExport"] = pd.to_numeric(work["ImportExport"], errors="coerce").astype("Int64")

    merged = work.merge(
        ids,
        left_on=["ncm", "ImportExport"],
        right_on=["NCM", "ImportExport"],
        how="left",
    )

    valid_mask = merged["IDIndicePrincipal"].notna()
    df_valid = merged.loc[valid_mask].drop(columns=["NCM"]).copy()

    if valid_mask.all():
        return df_valid, empty_dropped()

    dropped = (
        merged.loc[~valid_mask, ["ncm", "ImportExport"]]
        .drop_duplicates()
        .assign(
            IDIndicePrincipal=pd.NA,
            reason=DropReason.NCM_NOT_MAPPED.value,
            detail="par (ncm, ImportExport) ausente em IDS_comex",
        )
    )
    return df_valid, dropped[DROPPED_COLUMNS]


# =====================================================================
# F4 — top N% por peso (agrega para FOB_80, CIF_80)
# =====================================================================
def filter_top_percent(
    df: pd.DataFrame,
    threshold: float = 0.8,
    date_col: str = "Data",
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Retém os países que somam até `threshold` da participação por peso e
    devolve a série já agregada com FOB_80, CIF_80 e FOB_100 por (ID, Data).

    O retorno é o equivalente ao top_n_percent legado, mas sem efeitos colaterais
    (não exporta XLSX nem dorme). Países descartados aparecem em df_dropped
    apenas como contagem agregada por ID.
    """
    if df.empty:
        return df.copy(), empty_dropped()

    work = df.copy()
    has_freight = "frete" in work.columns and "seguro" in work.columns
    if has_freight:
        work["valor_cif"] = work["valor"] + work["frete"] + work["seguro"]
    else:
        work["valor_cif"] = np.nan

    grp = [ID_COL, date_col]
    pais_grp = grp + ["pais_id"]

    by_country = work.groupby(pais_grp, as_index=False).agg(
        cif_country=("valor_cif", "sum"),
        fob_country=("valor", "sum"),
        kg_country=("peso", "sum"),
    )

    by_country = by_country.sort_values(grp + ["kg_country"], ascending=[True, True, False])

    by_country["cum_kg"] = by_country.groupby(grp)["kg_country"].cumsum()
    total_kg = by_country.groupby(grp)["kg_country"].transform("sum")
    by_country["kg_ratio"] = np.where(total_kg > 0, by_country["cum_kg"] / total_kg, 0.0)

    prev_ratio = by_country.groupby(grp)["kg_ratio"].shift(fill_value=0.0)
    keep = prev_ratio < threshold

    kept = by_country.loc[keep].copy()
    dropped_n = int((~keep).sum())

    aggregated = kept.groupby(grp, as_index=False).agg(
        cum_cif=("cif_country", "sum"),
        cum_fob=("fob_country", "sum"),
        cum_kg=("kg_country", "sum"),
        pais_id_CUMUL=("pais_id", lambda s: list(s.astype(str))),
    )

    total_per_group = work.groupby(grp, as_index=False).agg(
        total_fob=("valor", "sum"),
        total_kg=("peso", "sum"),
    )
    aggregated = aggregated.merge(total_per_group, on=grp, how="left")

    aggregated["FOB_100"] = np.where(
        aggregated["total_kg"] > 0, aggregated["total_fob"] / aggregated["total_kg"], 0.0
    )
    aggregated["FOB_80"] = np.where(
        aggregated["cum_kg"] > 0, aggregated["cum_fob"] / aggregated["cum_kg"], 0.0
    )
    aggregated["CIF_80"] = np.where(
        aggregated["cum_kg"] > 0, aggregated["cum_cif"] / aggregated["cum_kg"], 0.0
    )

    result = aggregated[[ID_COL, date_col, "FOB_100", "FOB_80", "CIF_80", "pais_id_CUMUL"]].copy()
    result = result.sort_values(grp).reset_index(drop=True)

    if dropped_n == 0:
        return result, empty_dropped()

    drops_per_id = by_country.loc[~keep].groupby(ID_COL).size()
    dropped = drops_per_id.reset_index(name="_n").assign(
        ncm=pd.NA,
        ImportExport=pd.NA,
        reason=DropReason.NOT_IN_TOP_PERCENT.value,
        detail=lambda d: d["_n"].map(
            lambda n: f"{n} país(es) descartado(s) por estarem fora do top {int(threshold * 100)}% por peso"
        ),
    )
    return result, dropped[DROPPED_COLUMNS]


# =====================================================================
# F2 — histórico mínimo
# =====================================================================
def filter_min_history(
    df: pd.DataFrame,
    min_months: int = 24,
    date_col: str = "Data",
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Mantém IDs com pelo menos `min_months` meses distintos."""
    if df.empty:
        return df.copy(), empty_dropped()

    months_per_id = df.groupby(ID_COL)[date_col].transform("nunique")
    valid_mask = months_per_id >= min_months
    df_valid = df.loc[valid_mask].copy()

    if valid_mask.all():
        return df_valid, empty_dropped()

    counts = df.loc[~valid_mask].groupby(ID_COL)[date_col].nunique()
    dropped = counts.reset_index(name="_n").assign(
        ncm=pd.NA,
        ImportExport=pd.NA,
        reason=DropReason.INSUFFICIENT_HISTORY.value,
        detail=lambda d: d["_n"].map(lambda n: f"apenas {n} mês(es) (< {min_months})"),
    )
    return df_valid, dropped[DROPPED_COLUMNS]


# =====================================================================
# F3 — gap interno máximo
# =====================================================================
def filter_max_gap(
    df: pd.DataFrame,
    max_gap: int = 3,
    date_col: str = "Data",
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Descarta IDs cujo maior buraco interno consecutivo > max_gap meses."""
    if df.empty:
        return df.copy(), empty_dropped()

    work = df[[ID_COL, date_col]].drop_duplicates().copy()
    work[date_col] = pd.to_datetime(work[date_col]).dt.to_period("M").dt.to_timestamp()
    work = work.sort_values([ID_COL, date_col])

    period_int = work[date_col].dt.to_period("M").astype("int64")
    work["gap"] = period_int.groupby(work[ID_COL]).diff().sub(1)

    max_gap_per_id = work.groupby(ID_COL)["gap"].max().fillna(0).astype(int)
    bad_ids = max_gap_per_id[max_gap_per_id > max_gap]

    df_valid = df.loc[~df[ID_COL].isin(bad_ids.index)].copy()
    if bad_ids.empty:
        return df_valid, empty_dropped()

    dropped = bad_ids.reset_index(name="_gap").assign(
        ncm=pd.NA,
        ImportExport=pd.NA,
        reason=DropReason.GAP_TOO_LARGE.value,
        detail=lambda d: d["_gap"].map(lambda g: f"maior gap = {g} mês(es) (> {max_gap})"),
    )
    return df_valid, dropped[DROPPED_COLUMNS]
