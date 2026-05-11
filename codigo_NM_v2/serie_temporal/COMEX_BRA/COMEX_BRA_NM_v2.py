"""COMEX_BRA_NM v2 — herda do pipeline NM v2 e mantém a coleta original do Brasil.

A coleta (web scraping do portal COMEX), o tratamento específico do Brasil e
o `_get_country_name` são idênticos à versão atual. A única diferença é que
agora a classe consome `ComexPipelineNMv2`, que aplica:
  • filtros explícitos antes do cálculo (silver + dropped)
  • cálculo modular sem revisão automática
  • upload restrito a meses estritamente novos (sem re-publicar mês existente)
"""

import ssl
import sys
import warnings
from datetime import datetime
from io import BytesIO
from pathlib import Path
from typing import Dict, List

import pandas as pd
import requests
from bs4 import BeautifulSoup

# Adiciona o diretório serie_temporal ao path para importar a base v2
_PROJECT_ROOT = Path(__file__).parent.parent.parent.parent
_SERIE_TEMPORAL = _PROJECT_ROOT / "NM" / "serie_temporal"
if str(_SERIE_TEMPORAL) not in sys.path:
    sys.path.insert(0, str(_SERIE_TEMPORAL))

from costdrivers_comex_NM_v2 import ComexPipelineNMv2
from nm_config import NMConfig

warnings.filterwarnings("ignore", category=UserWarning, module="bs4")
requests.packages.urllib3.disable_warnings(  # noqa: SLF001
    requests.packages.urllib3.exceptions.InsecureRequestWarning
)


class COMEX_BRA_NM_v2(ComexPipelineNMv2):
    """Pipeline COMEX Brasil — Nova Metodologia v2."""

    BASE_URL = (
        "https://www.gov.br/produtividade-e-comercio-exterior/pt-br/assuntos/"
        "comercio-exterior/estatisticas/base-de-dados-bruta"
    )

    def __init__(self, config: NMConfig = None, start_date=None, use_azure: bool = True, developing: bool = False):
        super().__init__(
            iso_code="BRA",
            config=config,
            start_date=start_date,
            data_contract_path="data-contract.yaml",
            ids_table_path="IDS_comex.xlsx",
            use_azure=use_azure,
            developing=developing,
        )
        ssl._create_default_https_context = ssl._create_unverified_context  # noqa: SLF001

    # =================================================================
    # Coleta (web scraping do portal COMEX)
    # =================================================================
    def _get_country_name(self) -> str:
        return "Brasil"

    def _get_available_files(self) -> List[Dict[str, str]]:
        try:
            page = requests.get(self.BASE_URL, verify=False, timeout=30)
            page.raise_for_status()
        except requests.RequestException as exc:
            self.logger.error("Erro ao acessar o portal COMEX: %s", exc)
            return []

        soup = BeautifulSoup(page.content, "html.parser")
        years = {str(y) for y in range(self.start_date.year, datetime.now().year + 1)}

        files = []
        for link in soup.find_all("a"):
            href = link.get("href", "")
            if (
                "ncm" in href.lower()
                and "csv" in href.lower()
                and any(y in href for y in years)
            ):
                files.append({
                    "url": href,
                    "ImportExport": "export" if "EXP" in href.upper() else "import",
                    "year": next((y for y in years if y in href), None),
                })
        self.logger.info("Encontrados %s arquivos no portal COMEX", len(files))
        return files

    def _download_file(self, file_info: Dict[str, str]) -> bytes:
        try:
            response = requests.get(file_info["url"], verify=False, stream=True, timeout=60)
            response.raise_for_status()
            return response.content
        except requests.RequestException as exc:
            self.logger.error("Erro ao baixar %s: %s", file_info["url"], exc)
            return None

    def _process_ie_type(self, ie_type: str) -> pd.DataFrame:
        files = [f for f in self._get_available_files() if f["ImportExport"] == ie_type]
        if not files:
            self.logger.warning("Nenhum arquivo encontrado para %s", ie_type)
            return pd.DataFrame()

        dfs = []
        for info in files:
            self.logger.info("Processando %s — Ano %s", ie_type.upper(), info["year"])
            csv_bytes = self._download_file(info)
            if csv_bytes is None:
                continue
            try:
                df = pd.read_csv(BytesIO(csv_bytes), sep=";", encoding="utf-8", dtype=str)
                df["Data"] = pd.to_datetime(
                    df["CO_MES"].astype(str).str.zfill(2) + "-" + df["CO_ANO"].astype(str),
                    format="%m-%Y",
                )
                df["ImportExport"] = 1 if ie_type == "import" else 0
                dfs.append(df)
            except Exception as exc:  # noqa: BLE001
                self.logger.error("Erro ao processar CSV %s: %s", info["year"], exc)

        if not dfs:
            return pd.DataFrame()
        return pd.concat(dfs, ignore_index=True)

    def collect_import_data(self) -> pd.DataFrame:
        return self._process_ie_type("import")

    def collect_export_data(self) -> pd.DataFrame:
        return self._process_ie_type("export")

    # =================================================================
    # Tratamento específico (idêntico à versão atual)
    # =================================================================
    def _country_specific_treatment(self, df: pd.DataFrame) -> pd.DataFrame:
        if "Data" not in df.columns and {"CO_MES", "CO_ANO"}.issubset(df.columns):
            df["Data"] = pd.to_datetime(
                df["CO_MES"].astype(str).str.zfill(2) + "-" + df["CO_ANO"].astype(str),
                format="%m-%Y",
            )
        for col in ("CO_MES", "CO_ANO"):
            if col in df.columns:
                df = df.drop(columns=col)
        return df


def main(do_upload: bool = False):
    """Execução local. Por padrão NÃO sobe nada na plataforma — gera apenas
    silver, dropped e gold_NM_v2 em disco para validação.
    """
    today = datetime.now()
    start = (today.replace(day=1) - pd.DateOffset(months=5)).to_pydatetime()

    pipeline = COMEX_BRA_NM_v2(start_date=start, developing=True)

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
        f"\nPipeline v2 concluído (developing).\n"
        f"  silver:    {n_silver} linhas\n"
        f"  dropped:   {n_dropped} linhas\n"
        f"  gold v2:   {n_gold} linhas\n"
        f"  upload:    {'SIM' if do_upload else 'PULADO (use --with-upload para subir)'}"
    )
