"""
Configuração centralizada do COMEX NM v2.

Toda regra de negócio do pipeline (filtros, interpolação, comportamento de
revisão) vive aqui. Cada país pode passar uma instância customizada para o
pipeline; os valores padrão refletem as decisões alinhadas com o cliente.
"""

from dataclasses import dataclass, asdict


@dataclass
class NMConfig:
    """Parâmetros operacionais do pipeline NM v2."""

    # ---------- Filtros (F1..F5) ----------
    min_months_required: int = 24
    max_internal_gap_months: int = 3
    top_percent_threshold: float = 0.8

    # ---------- Cálculo / interpolação ----------
    max_internal_interpolation: int = 3
    max_tail_extrapolation: int = 3
    stl_seasonal_period: int = 13
    stl_outlier_zscore: float = 2.5
    historic_cutoff_date: str = "2022-01-01"

    # ---------- Comportamento ----------
    revise_published_months: bool = False
    hold_suspicious_outliers: bool = True
    overwrite_recent_history: bool = False

    # ---------- Upload (opt-in) ----------
    # Mantido como False por segurança: sem aprovação explícita, nada é enviado à plataforma.
    # Para habilitar: NMConfig(allow_upload=True)  — só funciona se developing=False.
    allow_upload: bool = False

    # ---------- Auditoria ----------
    save_dropped_dataframe: bool = True

    def to_dict(self) -> dict:
        return asdict(self)
