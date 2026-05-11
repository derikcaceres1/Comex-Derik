"""
COMEX NM v2 — pipeline refatorado com filtros explícitos e zero revisão.

Diferenças principais em relação ao costdrivers_comex_NM.py original:

1. Configuração centralizada (NMConfig).
2. Fase filter_data explícita antes do cálculo, com:
     • silver.parquet     — IDs válidos prontos para o cálculo
     • dropped.parquet    — IDs descartados com motivo (auditável)
3. Cálculo modular, em funções curtas e específicas. Outliers suspeitos são
   apenas marcados e segurados num relatório, nunca substituídos por
   estimativa estatística.
4. Histórico atualizado em modo append-only por padrão (sem sobrescrever
   meses já publicados).
5. Fase prepare_upload separada: gera to_upload.parquet contendo apenas
   meses estritamente posteriores ao último mês já existente na plataforma.
6. Upload sobe somente o to_upload.parquet — não há janela de revisão.

A classe herda de ComexPipelineNM apenas para reaproveitar helpers de
storage, data-contract e tabela de IDs. Toda fase de regra de negócio é
sobrescrita.

Subclasses por país precisam apenas:
    • implementar _get_country_name()
    • implementar collect_import_data() / collect_export_data() (ou usar
      iso_database='EUR' para histórico compartilhado)
    • opcionalmente passar um NMConfig customizado ao __init__
    • opcionalmente sobrescrever _country_specific_treatment()
"""

from __future__ import annotations

import json
import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Tuple

import numpy as np
import pandas as pd
from statsmodels.tsa.seasonal import STL

# Garante que módulos auxiliares são encontrados
_HERE = Path(__file__).parent
if str(_HERE) not in sys.path:
    sys.path.insert(0, str(_HERE))

from costdrivers_comex_NM import ComexPipelineNM
from nm_config import NMConfig
from nm_filters import (
    ID_COL,
    empty_dropped,
    filter_invalid_rows,
    filter_max_gap,
    filter_min_history,
    filter_ncm_mapped,
    filter_top_percent,
)
from nm_reasons import DROPPED_COLUMNS, DropReason

logger = logging.getLogger(__name__)


# =====================================================================
# Funções de cálculo — pequenas, puras, fáceis de testar.
# =====================================================================

def reindex_full_monthly_range(df: pd.DataFrame) -> pd.DataFrame:
    """Garante uma linha por mês entre o primeiro e o último Data por ID."""
    if df.empty:
        return df.copy()

    pieces = []
    for id_, g in df.groupby(ID_COL, sort=False):
        g = g.sort_values("Data").drop_duplicates(subset=["Data"], keep="last")
        full = pd.date_range(g["Data"].min(), g["Data"].max(), freq="MS")
        g = g.set_index("Data").reindex(full).rename_axis("Data").reset_index()
        g[ID_COL] = id_
        pieces.append(g)
    return pd.concat(pieces, ignore_index=True)


def interpolate_internal_gaps(
    df: pd.DataFrame, value_col: str, limit: int = 3
) -> pd.DataFrame:
    """Interpolação linear estritamente interna por ID (não toca em cauda)."""
    if df.empty:
        return df.copy()

    df = df.sort_values([ID_COL, "Data"]).copy()
    df[value_col] = df.groupby(ID_COL, sort=False)[value_col].transform(
        lambda s: s.interpolate(method="linear", limit_area="inside", limit=limit)
    )
    return df


def interpolate_tail(
    df: pd.DataFrame, value_col: str, limit: int = 3, window: int = 3
) -> pd.DataFrame:
    """Extrapola até `limit` meses após o último valor válido com média móvel.

    Acima de `limit` meses na cauda, mantém NaN — alinhado com a regra de não
    inventar dado além do limite acordado com o cliente.
    """
    if df.empty:
        return df.copy()

    df = df.sort_values([ID_COL, "Data"]).copy()

    def fill_tail(s: pd.Series) -> pd.Series:
        v = s.to_numpy(dtype=float, copy=True)
        valid = np.where(~np.isnan(v))[0]
        if valid.size == 0:
            return s
        last = valid[-1]
        n = min(limit, len(v) - 1 - last)
        for k in range(1, n + 1):
            w = v[max(0, last + k - window) : last + k]
            w = w[~np.isnan(w)]
            if w.size > 0:
                v[last + k] = w.mean()
        return pd.Series(v, index=s.index)

    df[value_col] = df.groupby(ID_COL, sort=False)[value_col].transform(fill_tail)
    return df


def decompose_stl_per_id(
    df: pd.DataFrame, value_col: str, period: int = 13
) -> pd.DataFrame:
    """Aplica STL por ID. Adiciona colunas trend, seasonality, residuals."""
    if df.empty:
        return df.assign(trend=np.nan, seasonality=np.nan, residuals=np.nan)

    period = period if period % 2 == 1 else period - 1
    pieces = []
    for id_, g in df.groupby(ID_COL, sort=False):
        g = g.sort_values("Data").set_index("Data").copy()
        if len(g) < 2 * period or g[value_col].isna().any():
            g["trend"] = np.nan
            g["seasonality"] = np.nan
            g["residuals"] = np.nan
        else:
            try:
                res = STL(g[value_col].astype(float), seasonal=period, robust=True).fit()
                g["trend"] = res.trend
                g["seasonality"] = res.seasonal
                g["residuals"] = res.resid
            except Exception as exc:  # noqa: BLE001
                logger.warning("STL falhou para ID %s: %s", id_, exc)
                g["trend"] = np.nan
                g["seasonality"] = np.nan
                g["residuals"] = np.nan
        pieces.append(g.reset_index())
    return pd.concat(pieces, ignore_index=True)


def mark_suspicious_outliers(
    df: pd.DataFrame, z_threshold: float = 2.5
) -> pd.DataFrame:
    """Marca pontos com |z-score do residual| > threshold. Não substitui."""
    if df.empty or "residuals" not in df.columns:
        df = df.copy()
        df["residuals_zscore"] = np.nan
        df["is_outlier_suspicious"] = False
        return df

    grp = df.groupby(ID_COL, sort=False)["residuals"]
    mean = grp.transform("mean")
    std = grp.transform("std")
    z = (df["residuals"] - mean).div(std)

    df = df.copy()
    df["residuals_zscore"] = z
    df["is_outlier_suspicious"] = z.abs().gt(z_threshold).fillna(False)
    return df


def compute_alpha(
    silver: pd.DataFrame,
    fob_col: str = "FOB_80",
    cif_col: str = "CIF_80",
) -> pd.DataFrame:
    """alpha = CIF/FOB onde CIF > 0. Retorna apenas (ID, Data, alpha)."""
    if silver.empty:
        return pd.DataFrame(columns=[ID_COL, "Data", "alpha"])

    sub = silver.loc[silver[cif_col] > 0, [ID_COL, "Data", fob_col, cif_col]].copy()
    sub["alpha"] = sub[cif_col].div(sub[fob_col])
    return sub[[ID_COL, "Data", "alpha"]]


def apply_alpha(fob_df: pd.DataFrame, alpha_df: pd.DataFrame) -> pd.DataFrame:
    """Multiplica Valor (FOB) por alpha do mesmo (ID, Data) para gerar Valor_Cif.

    Onde alpha não existe, Valor_Cif fica NaN — a projeção futura de CIF é
    feita no banco de dados, fora deste pipeline.
    """
    if fob_df.empty:
        return fob_df.assign(Valor_Cif=np.nan)

    alpha_renamed = alpha_df.rename(columns={ID_COL: "ID"})
    out = fob_df.merge(alpha_renamed, on=["ID", "Data"], how="left")
    out["Valor_Cif"] = out["Valor"].mul(out["alpha"])
    return out[["ID", "Data", "Valor", "Valor_Cif"]].copy()


# =====================================================================
# Pipeline principal
# =====================================================================

class ComexPipelineNMv2(ComexPipelineNM):
    """Pipeline COMEX NM v2 — modular, com filtros explícitos e sem revisão.

    Herda de ComexPipelineNM apenas para reaproveitar:
      • storage helpers (_save_to_storage, _load_from_storage, ...)
      • data-contract loader e normalize_columns
      • load_ids_table, _country_specific_treatment, _standardize_data_types
      • collect_import_data / collect_export_data (mantém compatibilidade)

    Sobrescreve:
      • update_historical    — append-only por padrão
      • calculate            — modular, sem substituição automática
      • upload               — sobe apenas o to_upload.parquet

    Acrescenta:
      • filter_data          — fase explícita de filtros
      • prepare_upload       — gera o arquivo do que sobe na plataforma
    """

    PIPELINE_VERSION = "2.0.0"

    def __init__(
        self,
        iso_code: str,
        config: Optional[NMConfig] = None,
        start_date: Optional[datetime] = None,
        data_contract_path: Optional[str] = None,
        ids_table_path: Optional[str] = None,
        storage_base_path: str = "staging/comex",
        use_azure: bool = True,
        developing: bool = False,
        iso_database: Optional[str] = None,
    ):
        self.config = config or NMConfig()
        super().__init__(
            iso_code=iso_code,
            start_date=start_date,
            data_contract_path=data_contract_path,
            ids_table_path=ids_table_path,
            storage_base_path=storage_base_path,
            use_azure=use_azure,
            threshold_percent=self.config.top_percent_threshold,
            min_months_required=self.config.min_months_required,
            developing=developing,
            iso_database=iso_database,
        )
        self.silver_df: Optional[pd.DataFrame] = None
        self.dropped_df: Optional[pd.DataFrame] = None
        self.suspicious_df: Optional[pd.DataFrame] = None
        self.gold_df: Optional[pd.DataFrame] = None
        self.to_upload_df: Optional[pd.DataFrame] = None
        self._manifest: dict = {
            "iso_code": self.iso_code,
            "iso_database": self.iso_database,
            "version": self.PIPELINE_VERSION,
            "config": self.config.to_dict(),
            "started_at": datetime.now().isoformat(timespec="seconds"),
        }
        self.logger.info("Pipeline NM v2 inicializado (versão %s)", self.PIPELINE_VERSION)

    # =================================================================
    # FASE: Atualização do histórico (append-only)
    # =================================================================
    def update_historical(self, update_months: int = 3):
        """Atualiza historical.parquet em modo append-only por padrão.

        Quando overwrite_recent_history=True na config, delega para o
        comportamento antigo da classe pai (substitui últimos N meses).
        """
        self.logger.info("=== INICIANDO ATUALIZAÇÃO DO HISTÓRICO (v2) ===")

        if self.config.overwrite_recent_history:
            self.logger.warning("overwrite_recent_history=True — usando comportamento legado.")
            return super().update_historical(update_months=update_months)

        existing = self._load_historical_data()

        if self.iso_database == "EUR":
            self.historical_df = existing
            self.logger.info("=== ATUALIZAÇÃO CONCLUÍDA (EUR — somente leitura) ===\n")
            return

        if self.raw_df is None or self.raw_df.empty:
            self.historical_df = existing
            self.logger.info("=== ATUALIZAÇÃO CONCLUÍDA (sem dados novos) ===\n")
            return

        if existing.empty:
            self.logger.info("Histórico inexistente — gravando dados brutos no schema atual.")
            new_in_schema = self.raw_df.copy()
        else:
            new_in_schema = self._raw_to_historical_schema(self.raw_df, existing_historical=existing)

        if new_in_schema.empty:
            self.historical_df = existing
            self.logger.info("=== ATUALIZAÇÃO CONCLUÍDA (conversão vazia) ===\n")
            return

        merged = self._append_only(existing, new_in_schema)
        self._save_to_storage(merged, self.historical_data_path, "historical.parquet")
        self.historical_df = merged
        added = len(merged) - len(existing)
        self.logger.info(
            "Append-only concluído: +%s linhas (total: %s)", added, len(merged)
        )
        self._manifest["historical_rows_added"] = int(added)
        self._manifest["historical_rows_total"] = int(len(merged))
        self.logger.info("=== ATUALIZAÇÃO CONCLUÍDA ===\n")

    @staticmethod
    def _historical_keys(df: pd.DataFrame) -> List[str]:
        if "Data" in df.columns:
            date_keys = ["Data"]
        elif "CO_ANO" in df.columns and "CO_MES" in df.columns:
            date_keys = ["CO_ANO", "CO_MES"]
        else:
            return []
        return date_keys + [c for c in ("ncm", "pais_id", "ImportExport") if c in df.columns]

    def _append_only(
        self, existing: pd.DataFrame, new_df: pd.DataFrame
    ) -> pd.DataFrame:
        """Anexa apenas linhas com chave inédita; nunca sobrescreve linhas existentes."""
        if existing.empty:
            return new_df.copy()
        if new_df.empty:
            return existing.copy()

        keys = self._historical_keys(existing)
        if not keys:
            self.logger.warning("Não foi possível determinar chave; preservando histórico existente.")
            return existing.copy()

        marker = existing[keys].drop_duplicates().assign(_already=True)
        candidate = new_df.merge(marker, on=keys, how="left")
        only_new = candidate.loc[candidate["_already"].isna()].drop(columns=["_already"])

        if only_new.empty:
            self.logger.info("Nenhuma chave nova no histórico — append-only sem alterações.")
            return existing.copy()

        combined = pd.concat([existing, only_new], ignore_index=True).sort_values(keys)
        return combined.reset_index(drop=True)

    # =================================================================
    # FASE: filter_data (NOVO)
    # =================================================================
    def filter_data(self) -> pd.DataFrame:
        """Aplica F1..F5 antes do cálculo, gerando silver e relatório de descartes."""
        self.logger.info("=== INICIANDO FASE: FILTER_DATA ===")

        if self.historical_df is None or self.historical_df.empty:
            self.logger.error("historical_df vazio — filter_data abortado.")
            self.silver_df = pd.DataFrame()
            self.dropped_df = empty_dropped()
            return self.silver_df

        df = self.historical_df.copy()
        if "pais_id" not in df.columns and "pais_name" in df.columns:
            df = df.rename(columns={"pais_name": "pais_id"})

        required = ["ncm", "Data", "pais_id", "valor", "frete", "seguro", "peso", "ImportExport"]
        missing = [c for c in required if c not in df.columns]
        if missing:
            self.logger.error("Colunas obrigatórias ausentes: %s", missing)
            self.silver_df = pd.DataFrame()
            self.dropped_df = empty_dropped()
            return self.silver_df

        ids_table = self.load_ids_table()

        drops: List[pd.DataFrame] = []

        # F5 — linhas inválidas
        df, dr = filter_invalid_rows(df)
        drops.append(dr)
        self.logger.info("F5 (invalid_rows): %s linhas restantes, %s descartadas", len(df), len(dr))

        # F1 — NCM mapeado (anexa IDIndicePrincipal)
        df, dr = filter_ncm_mapped(df, ids_table)
        drops.append(dr)
        self.logger.info("F1 (ncm_mapped): %s linhas com ID, %s NCMs descartados", len(df), len(dr))

        # F4 — top 80% (agrega para FOB_80, CIF_80, FOB_100 por ID, Data)
        df, dr = filter_top_percent(df, threshold=self.config.top_percent_threshold)
        drops.append(dr)
        self.logger.info("F4 (top_percent): %s pontos agregados, %s IDs com países descartados",
                         len(df), len(dr))

        # F2 — histórico mínimo
        df, dr = filter_min_history(df, min_months=self.config.min_months_required)
        drops.append(dr)
        self.logger.info("F2 (min_history): %s pontos restantes, %s IDs descartados",
                         len(df), len(dr))

        # F3 — gap interno máximo
        df, dr = filter_max_gap(df, max_gap=self.config.max_internal_gap_months)
        drops.append(dr)
        self.logger.info("F3 (max_gap): %s pontos restantes, %s IDs descartados", len(df), len(dr))

        self.silver_df = df.reset_index(drop=True)
        non_empty_drops = [d for d in drops if not d.empty]
        self.dropped_df = (
            pd.concat(non_empty_drops, ignore_index=True)[DROPPED_COLUMNS]
            if non_empty_drops else empty_dropped()
        )

        n_valid_ids = self.silver_df[ID_COL].nunique() if not self.silver_df.empty else 0
        n_dropped_ids = (
            self.dropped_df[ID_COL].dropna().nunique() if not self.dropped_df.empty else 0
        )
        self.logger.info("Filtros concluídos: %s IDs válidos, %s IDs/NCMs descartados",
                         n_valid_ids, n_dropped_ids)

        if self.config.save_dropped_dataframe and not self.dropped_df.empty:
            date_str = datetime.now().strftime("%Y-%m-%d")
            self._save_to_storage(self.dropped_df, self.silver_path, f"dropped_{date_str}.parquet")
        if not self.silver_df.empty:
            date_str = datetime.now().strftime("%Y-%m-%d")
            self._save_to_storage(self.silver_df, self.silver_path, f"silver_{date_str}.parquet")

        self._manifest["filter_summary"] = {
            "ids_valid": int(n_valid_ids),
            "ids_dropped": int(n_dropped_ids),
            "by_reason": (
                self.dropped_df.groupby("reason").size().to_dict()
                if not self.dropped_df.empty else {}
            ),
        }
        self.logger.info("=== FASE FILTER_DATA CONCLUÍDA ===\n")
        return self.silver_df

    # =================================================================
    # FASE: calculate (refatorada)
    # =================================================================
    def calculate(self) -> pd.DataFrame:
        """Pipeline de cálculo modular sem substituição automática de outliers.

        Etapas:
          1. compute_alpha (do silver, antes de qualquer filtro temporal)
          2. série FOB → reindex contínuo → interpolação interna (limit=3)
          3. STL → marcação de outliers suspeitos (sem substituição)
          4. recorte para Type='target' (a partir de historic_cutoff_date)
          5. extrapolação de cauda (limit=3)
          6. apply_alpha → Valor_Cif
        """
        self.logger.info("=== INICIANDO FASE: CALCULATE (v2) ===")

        if self.silver_df is None or self.silver_df.empty:
            self.logger.error("silver_df vazio — calculate abortado.")
            self.gold_df = pd.DataFrame()
            return self.gold_df

        cutoff = pd.Timestamp(self.config.historic_cutoff_date)
        silver = self.silver_df.copy()
        silver["Data"] = pd.to_datetime(silver["Data"])
        silver["Type"] = np.where(silver["Data"] >= cutoff, "target", "historic")

        alpha = compute_alpha(silver)
        self.logger.info("Alpha calculado para %s IDs", alpha[ID_COL].nunique() if not alpha.empty else 0)

        fob = (
            silver[[ID_COL, "Data", "FOB_80", "Type"]]
            .rename(columns={"FOB_80": "Valor"})
            .copy()
        )
        fob["Valor"] = fob["Valor"].astype(float).replace(0, np.nan)

        fob = reindex_full_monthly_range(fob)
        fob["Type"] = np.where(fob["Data"] >= cutoff, "target", "historic")

        fob = interpolate_internal_gaps(fob, "Valor", limit=self.config.max_internal_interpolation)
        self.logger.info("Interpolação interna aplicada (limit=%s)", self.config.max_internal_interpolation)

        fob_decomposed = decompose_stl_per_id(
            fob, value_col="Valor", period=self.config.stl_seasonal_period
        )
        fob_marked = mark_suspicious_outliers(fob_decomposed, z_threshold=self.config.stl_outlier_zscore)

        suspicious = fob_marked.loc[fob_marked["is_outlier_suspicious"], [ID_COL, "Data", "Valor", "residuals_zscore"]]
        self.suspicious_df = suspicious.reset_index(drop=True).copy()
        if not self.suspicious_df.empty:
            self.logger.info(
                "Outliers suspeitos marcados: %s pontos em %s IDs (segurados sem substituição)",
                len(self.suspicious_df), self.suspicious_df[ID_COL].nunique(),
            )

        target = fob_marked.loc[fob_marked["Type"] == "target", [ID_COL, "Data", "Valor"]].copy()

        target = interpolate_tail(target, "Valor", limit=self.config.max_tail_extrapolation)
        self.logger.info("Extrapolação de cauda aplicada (limit=%s)", self.config.max_tail_extrapolation)

        gold_fob = target.rename(columns={ID_COL: "ID"})
        gold = apply_alpha(gold_fob, alpha)
        gold = gold.dropna(subset=["Valor"]).reset_index(drop=True)

        self.gold_df = gold
        date_str = datetime.now().strftime("%Y-%m-%d")
        self._save_to_storage(self.gold_df, self.gold_path, f"gold_NM_v2_{date_str}.parquet")

        if self.config.hold_suspicious_outliers and not self.suspicious_df.empty:
            self._save_to_storage(self.suspicious_df, self.gold_path, f"suspicious_outliers_{date_str}.parquet")

        self._manifest["calculate_summary"] = {
            "gold_rows": int(len(self.gold_df)),
            "gold_ids": int(self.gold_df["ID"].nunique()) if not self.gold_df.empty else 0,
            "suspicious_points": int(len(self.suspicious_df)) if self.suspicious_df is not None else 0,
        }
        self.logger.info("=== FASE CALCULATE CONCLUÍDA: %s registros ===\n", len(self.gold_df))
        return self.gold_df

    # =================================================================
    # FASE: prepare_upload (NOVO)
    # =================================================================
    def prepare_upload(self) -> pd.DataFrame:
        """Filtra o gold para conter apenas meses estritamente novos por ID.

        Consulta a API Cost Drivers para descobrir o último mês já publicado
        de cada ID e devolve apenas os registros com Data > max_data.
        IDs não encontrados na API têm todos os meses considerados novos.
        """
        self.logger.info("=== INICIANDO FASE: PREPARE_UPLOAD ===")

        if self.gold_df is None or self.gold_df.empty:
            self.logger.warning("gold_df vazio — prepare_upload abortado.")
            self.to_upload_df = pd.DataFrame()
            return self.to_upload_df

        ids = self.gold_df["ID"].unique().tolist()
        api_max = self._fetch_api_max_data(ids)

        merged = self.gold_df.merge(api_max, on="ID", how="left")
        merged["max_data"] = merged["max_data"].fillna(pd.Timestamp("1900-01-01"))
        new_rows = merged.loc[merged["Data"] > merged["max_data"]].drop(columns=["max_data"])

        self.to_upload_df = new_rows.reset_index(drop=True)

        if self.config.save_to_upload_dataframe:
            date_str = datetime.now().strftime("%Y-%m-%d")
            self._save_to_storage(self.to_upload_df, self.gold_path, f"to_upload_{date_str}.parquet")

        n_ids = self.to_upload_df["ID"].nunique() if not self.to_upload_df.empty else 0
        self.logger.info(
            "Upload preparado: %s linhas em %s IDs (somente meses estritamente novos)",
            len(self.to_upload_df), n_ids,
        )
        self._manifest["upload_summary"] = {
            "rows_to_upload": int(len(self.to_upload_df)),
            "ids_to_upload": int(n_ids),
        }
        self.logger.info("=== FASE PREPARE_UPLOAD CONCLUÍDA ===\n")
        return self.to_upload_df

    # =================================================================
    # FASE: upload (sem revisão)
    # =================================================================
    def upload(self, ExcluirHistorico: str = "N", percentualOutlier: float = 4) -> bool:
        """Sobe to_upload_df para a API. Sem nenhuma janela adicional de revisão."""
        self.logger.info("=== INICIANDO FASE: UPLOAD (v2) ===")

        if self.to_upload_df is None or self.to_upload_df.empty:
            self.logger.info("Nada para subir — to_upload_df vazio.")
            return True

        try:
            from library.costdrivers import ApiAsync
        except Exception as exc:  # noqa: BLE001
            self.logger.error("ApiAsync indisponível: %s", exc)
            return False

        endpoint_cost = "https://api-costdrivers.gep.com/costdrivers-api"

        def get_token() -> str:
            headers = {
                "key": "070F0E8A-E0C6-4970-8865-480650C0D12C",
                "email": "datascience@datamark.com.br",
                "pass": "i2024nb4",
            }
            req = {"url": f"{endpoint_cost}/api/v1/Auth", "method": "GET"}
            return ApiAsync(True, [req], headers=headers).run()[0]["result"]["tokenCode"]

        token_headers = {"Authorization": "Bearer " + get_token()}
        df_up = self.to_upload_df.copy()
        df_up["Data"] = pd.to_datetime(df_up["Data"]).dt.strftime("01-%m-%Y")

        requests_payload = []
        for id_, group in df_up.groupby("ID"):
            payload_df = group.dropna(axis=1, how="all").copy()
            payload_df["Base"] = "0"
            payload_df["Explicativa"] = "0"
            json_data = payload_df.to_json(orient="records").replace("'", '"')
            json_seq = (
                str({"ID": str(id_), "ExcluirHistorico": ExcluirHistorico, "Origem": "Data Science"})
                .replace("'", '"')
            )
            requests_payload.append({
                "url": f"{endpoint_cost}/api/v1/DataScience/UpdateOption-9",
                "method": "put",
                "json": {
                    "opc": 9,
                    "idioma": "1",
                    "identity": "2258FB7E-7F19-483E-BBAE-8250973D3658",
                    "percentualOutlier": percentualOutlier,
                    "merchantID": "",
                    "json": json_data,
                    "jsonSeq": json_seq,
                },
                "ID": id_,
            })

        responses = self._upload_with_retry(ApiAsync, requests_payload, token_headers)
        success = sum(1 for r in responses if r and not r.get("Error"))
        self.logger.info("Upload concluído: %s/%s requisições com sucesso", success, len(responses))

        self._manifest["upload_result"] = {
            "requests_total": len(responses),
            "requests_success": int(success),
        }
        return success == len(responses)

    @staticmethod
    def _upload_with_retry(ApiAsync, requests_list, headers, max_retries: int = 3):
        responses = ApiAsync(True, requests_list, headers=headers).run()
        for _ in range(max_retries):
            failed_idx = [i for i, r in enumerate(responses) if r is None]
            if not failed_idx:
                break
            retry = [requests_list[i] for i in failed_idx]
            retry_resp = ApiAsync(True, retry, headers=headers).run()
            for i, r in zip(failed_idx, retry_resp):
                responses[i] = r
        return responses

    # =================================================================
    # Storage — força regravação (v1 pula quando tamanho bate)
    # =================================================================
    def _save_to_storage(self, df: pd.DataFrame, path: str, filename: str):
        full_path = Path(path)
        full_path.mkdir(parents=True, exist_ok=True)
        file_path = full_path / filename
        df.to_parquet(file_path, index=False, compression="gzip")
        self.logger.info("Salvo: %s (%s linhas)", file_path, len(df))

    # =================================================================
    # API helper — buscar max_data por ID
    # =================================================================
    def _fetch_api_max_data(self, ids: List[int]) -> pd.DataFrame:
        """Consulta a API Cost Drivers e devolve um DataFrame (ID, max_data)."""
        if not ids:
            return pd.DataFrame(columns=["ID", "max_data"])

        try:
            from library.costdrivers import ApiAsync
        except Exception as exc:  # noqa: BLE001
            self.logger.error("ApiAsync indisponível: %s", exc)
            return pd.DataFrame(columns=["ID", "max_data"])

        endpoint = "https://api-costdrivers.gep.com/costdrivers-api"

        def token() -> str:
            headers = {
                "key": "070F0E8A-E0C6-4970-8865-480650C0D12C",
                "email": "datascience@datamark.com.br",
                "pass": "i2024nb4",
            }
            req = {"url": f"{endpoint}/api/v1/Auth", "method": "GET"}
            return ApiAsync(True, [req], headers=headers).run()[0]["result"]["tokenCode"]

        data_max = pd.Timestamp.today().strftime("%d-%m-%Y")
        data_min = (pd.Timestamp.today() - pd.DateOffset(months=24)).strftime("%d-%m-%Y")

        requests_list = []
        for i in range(0, len(ids), 49):
            chunk = ids[i : i + 49]
            requests_list.append({
                "url": f"{endpoint}/api/v1/DataScience/option/11",
                "method": "get",
                "params": {
                    "ids": ",".join(map(str, chunk)),
                    "dataCalculo1": data_min,
                    "dataCalculo2": data_max,
                },
            })

        responses = ApiAsync(True, requests_list, headers={"Authorization": "Bearer " + token()}).run()
        frames = [pd.DataFrame(r["result"]) for r in responses if r and "result" in r and "Error" not in r]
        if not frames:
            return pd.DataFrame(columns=["ID", "max_data"])

        df = pd.concat(frames, ignore_index=True)
        if df.empty or "indicePrincipalID" not in df.columns:
            return pd.DataFrame(columns=["ID", "max_data"])

        df = df.query("base == 0")[["indicePrincipalID", "dataIndice"]].copy()
        df.columns = ["ID", "Data"]
        df["Data"] = pd.to_datetime(df["Data"], errors="coerce")
        df = df.dropna(subset=["Data"])

        return df.groupby("ID", as_index=False)["Data"].max().rename(columns={"Data": "max_data"})

    # =================================================================
    # Manifesto / auditoria
    # =================================================================
    def _save_manifest(self):
        if not self.config.save_run_manifest:
            return
        self._manifest["finished_at"] = datetime.now().isoformat(timespec="seconds")
        date_str = datetime.now().strftime("%Y-%m-%d")
        path = Path(self.gold_path)
        path.mkdir(parents=True, exist_ok=True)
        with open(path / f"manifest_v2_{date_str}.json", "w", encoding="utf-8") as f:
            json.dump(self._manifest, f, ensure_ascii=False, indent=2, default=str)
        self.logger.info("Manifest salvo: %s", path / f"manifest_v2_{date_str}.json")

    # =================================================================
    # Orquestração
    # =================================================================
    def run(self, skip_phases: Optional[List[str]] = None) -> pd.DataFrame:
        """Executa o pipeline v2 completo na ordem correta."""
        skip = set(skip_phases or [])
        self.logger.info("=" * 60)
        self.logger.info("PIPELINE COMEX NM v2 — %s", self.iso_code)
        self.logger.info("=" * 60)

        try:
            if "collect" not in skip:
                self.collect()

            if "update_historical" not in skip:
                self.update_historical()

            if "normalize_historical" not in skip:
                self.normalize_historical()

            if "filter_data" not in skip:
                self.filter_data()

            if "calculate" not in skip:
                self.calculate()

            if "prepare_upload" not in skip:
                self.prepare_upload()

            if "upload" not in skip:
                self.upload()

            self._save_manifest()
            self.logger.info("=" * 60)
            self.logger.info("PIPELINE v2 CONCLUÍDO")
            self.logger.info("=" * 60)
            return self.to_upload_df if self.to_upload_df is not None else pd.DataFrame()

        except Exception:
            self.logger.exception("Erro no pipeline v2")
            self._save_manifest()
            raise
