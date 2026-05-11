"""COMEX_ITA_NM v2 — herda do pipeline NM v2 e mantém a coleta original da Itália.

A coleta (histórico compartilhado EUR) e o tratamento específico do país são
idênticos à versão atual. A única diferença é que agora a classe consome
`ComexPipelineNMv2`, que aplica:
  • filtros explícitos antes do cálculo (silver + dropped)
  • cálculo idêntico ao v1 (preprocess + STL + IQR + last-month + negative)
  • histórico em modo append-only
  • upload idêntico ao v1, com guard opt-in (NMConfig.allow_upload)
"""

import sys
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

# Adiciona o diretório serie_temporal ao path para importar a base v2
_PROJECT_ROOT = Path(__file__).parent.parent.parent.parent
_SERIE_TEMPORAL = _PROJECT_ROOT / "NM" / "serie_temporal"
if str(_SERIE_TEMPORAL) not in sys.path:
    sys.path.insert(0, str(_SERIE_TEMPORAL))

from costdrivers_comex_NM_v2 import ComexPipelineNMv2
from nm_config import NMConfig


class COMEX_ITA_NM_v2(ComexPipelineNMv2):
    """Pipeline COMEX Itália — Nova Metodologia v2.

    Usa histórico compartilhado do EUR via `iso_database='EUR'`. A coleta de
    import/export é a herdada da base (lê o histórico compartilhado em vez de
    fazer scraping próprio).
    """

    def __init__(
        self,
        config: NMConfig = None,
        start_date=None,
        use_azure: bool = True,
        developing: bool = False,
    ):
        super().__init__(
            iso_code="ITA",
            config=config,
            start_date=start_date,
            data_contract_path="data-contract.yaml",
            ids_table_path="IDS_comex.xlsx",
            use_azure=use_azure,
            developing=developing,
            iso_database="EUR",
        )

    def _get_country_name(self) -> str:
        return "Itália"

    # =================================================================
    # Tratamento específico (idêntico ao v1)
    # =================================================================
    def _country_specific_treatment(self, df: pd.DataFrame) -> pd.DataFrame:
        if "PRODUCT_NC" in df.columns:
            df = df[df["PRODUCT_NC"] != "TOTAL"].copy()
            df = df[~df["PRODUCT_NC"].isnull()].copy()

        if "PERIOD" in df.columns:
            df["Data"] = pd.to_datetime(df["PERIOD"].astype(str), format="%Y%m", errors="coerce")
            df = df.drop(columns=["PERIOD"])

        if "Data" not in df.columns:
            self.logger.warning("Coluna Data não encontrada após normalização")
            return df

        if "PRODUCT_NC" in df.columns:
            df["PRODUCT_NC"] = pd.to_numeric(df["PRODUCT_NC"], errors="coerce").astype("Int64")
            df = df.dropna(subset=["PRODUCT_NC"])
            df = df.astype({"PRODUCT_NC": "int32"})

        df["frete"] = np.nan
        df["seguro"] = np.nan

        if "TRADE_TYPE" in df.columns:
            df = df.rename(columns={"TRADE_TYPE": "ImportExport"})
        if "REPORTER" in df.columns:
            df = df.drop(columns=["REPORTER"])

        return df


def main(do_upload: bool = False):
    """Execução local. Por padrão NÃO sobe nada na plataforma — gera apenas
    silver, dropped e gold_NM_v2 em disco para validação.
    """
    pipeline = COMEX_ITA_NM_v2(start_date=None, developing=True)
    skip = [] if do_upload else ["upload"]
    pipeline.run(skip_phases=skip)
    return pipeline


if __name__ == "__main__":
    import sys as _sys

    do_upload = "--with-upload" in _sys.argv
    p = main(do_upload=do_upload)
    n_silver = len(p.silver_df) if p.silver_df is not None else 0
    n_dropped = len(p.dropped_df) if p.dropped_df is not None else 0
    n_gold = len(p.gold_df) if p.gold_df is not None else 0
    print(
        f"\nPipeline v2 ITA concluído (developing).\n"
        f"  silver:    {n_silver} linhas\n"
        f"  dropped:   {n_dropped} linhas\n"
        f"  gold v2:   {n_gold} linhas\n"
        f"  upload:    {'SIM' if do_upload else 'PULADO (use --with-upload para subir)'}"
    )
