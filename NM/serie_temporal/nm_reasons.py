"""
Motivos de descarte de IDs/NCMs no pipeline NM v2.

Cada filtro produz linhas com um destes motivos no DataFrame de descarte,
permitindo auditoria completa do que ficou de fora do cálculo e por quê.
"""

from enum import Enum


class DropReason(str, Enum):
    NCM_NOT_MAPPED = "ncm_not_mapped"
    INSUFFICIENT_HISTORY = "insufficient_history"
    GAP_TOO_LARGE = "gap_too_large"
    NOT_IN_TOP_PERCENT = "not_in_top_percent"
    INVALID_ROW = "invalid_row"
    SUSPICIOUS_OUTLIER = "suspicious_outlier"
    NO_DATA_FOR_CALC = "no_data_for_calc"


REASON_DESCRIPTIONS = {
    DropReason.NCM_NOT_MAPPED: "NCM não está mapeado em IDS_comex",
    DropReason.INSUFFICIENT_HISTORY: "Série tem menos meses do que o mínimo exigido",
    DropReason.GAP_TOO_LARGE: "Buraco interno maior que o limite permitido",
    DropReason.NOT_IN_TOP_PERCENT: "País não compõe a participação acumulada exigida",
    DropReason.INVALID_ROW: "Linha com peso ou valor inválido",
    DropReason.SUSPICIOUS_OUTLIER: "Valor suspeito segurado para revisão humana",
    DropReason.NO_DATA_FOR_CALC: "Sem registros remanescentes para o cálculo",
}


DROPPED_COLUMNS = ["IDIndicePrincipal", "ncm", "ImportExport", "reason", "detail"]
