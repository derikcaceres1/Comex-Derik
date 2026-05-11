"""
Pipeline COMEX Japão (JPN) — Nova Metodologia.

Coleta dados de importação e exportação do Japão a partir do portal
e-stat (Statistics Bureau of Japan), disponibilizado pela Customs
and Tariff Bureau do Ministry of Finance.

Fonte: https://www.customs.go.jp/toukei/info/tsdl_e.htm
       → Commodity by Country → Import / Export

Os dados ficam no e-stat em formato "wide" (um CSV por seção/chapter,
com colunas Value-Jan … Value-Dec). O pipeline:
1. Descobre dinamicamente os statInfId de cada seção para o período
   desejado, navegando pela página de listagem do e-stat.
2. Baixa os 22 CSVs de Import e 22 de Export.
3. Transforma de wide (colunas mensais) para long (uma linha/mês).
4. Mapeia código de país → nome usando planilha auxiliar.
5. Entrega o DataFrame no schema padrão NM para as fases seguintes.

Observações:
- O Japão utiliza HS de 9 dígitos; os primeiros 8 são usados como NCM.
- Valores em Yen (¥), sem frete/seguro desagregados (análogo aos EUR).
- Pouquíssimos registros possuem HS confidencial (ex: "03XXXXXX") e
  são removidos automaticamente.
"""

import re
import time
from datetime import datetime, timedelta
from io import BytesIO
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import requests
import urllib3
import sys
from pathlib import Path

# Adicionar path do diretório serie_temporal ao sys.path para imports
project_root = Path(__file__).parent.parent.parent.parent
serie_temporal_path = project_root / "NM" / "serie_temporal"
if str(serie_temporal_path) not in sys.path:
    sys.path.insert(0, str(serie_temporal_path))

from costdrivers_comex_NM import ComexPipelineNM

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ──────────────────────────────────────────────────────────────────
# Constantes da fonte e-stat
# ──────────────────────────────────────────────────────────────────

# Página fixa do Ministério das Finanças — raramente muda
_TSDL_URL = "https://www.customs.go.jp/toukei/info/tsdl_e.htm"

# Parâmetros fixos do e-stat para "Commodity by Country"
_ESTAT_BASE = "https://www.e-stat.go.jp"
_ESTAT_FILE_PARAMS = {
    "page": "1",
    "layout": "datalist",
    "toukei": "00350300",
    "tstat": "000001013141",
    "cycle": "1",
    "tclass1": "000001013180",
    "cycle_facet": "cycle",
    "tclass3val": "0",
    "metadata": "1",
    "data": "1",
}

# tclass2 distingue Import vs Export
_TCLASS2_IMPORT = "000001013182"
_TCLASS2_EXPORT = "000001013181"

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}

# Mapeamento de mês (abreviação do CSV) → número
_MONTH_MAP = {
    "Jan": "01", "Feb": "02", "Mar": "03", "Apr": "04",
    "May": "05", "Jun": "06", "Jul": "07", "Aug": "08",
    "Sep": "09", "Oct": "10", "Nov": "11", "Dec": "12",
}


# ──────────────────────────────────────────────────────────────────
# Funções auxiliares de coleta (não dependem de instância)
# ──────────────────────────────────────────────────────────────────

def _resolve_listing_url(tclass2: str) -> str:
    """
    Obtém a URL de listagem (Import ou Export) de forma dinâmica a
    partir da página oficial do Ministério das Finanças, evitando
    hard-code de URLs do e-stat que podem mudar.

    Se não conseguir, monta a URL com os parâmetros conhecidos como
    fallback (é estável há anos, mas não garantido).
    """
    try:
        r = requests.get(_TSDL_URL, headers=_HEADERS, timeout=15, verify=False)
        r.raise_for_status()

        html = r.text.replace("&amp;", "&")
        pattern = rf'href="([^"]*tclass2={tclass2}[^"]*)"'
        match = re.search(pattern, html)
        if match:
            url = match.group(1)
            if url.startswith("/"):
                url = f"https://www.e-stat.go.jp{url}"
            return url
    except Exception:
        pass

    # Fallback: montar URL com parâmetros fixos
    params = _ESTAT_FILE_PARAMS.copy()
    params["tclass2"] = tclass2
    qs = "&".join(f"{k}={v}" for k, v in params.items())
    return f"{_ESTAT_BASE}/en/stat-search/files?{qs}"


def _discover_year_url(listing_url: str, year: int) -> Optional[str]:
    """
    Na página de listagem (que mostra todos os anos), descobre a URL
    da página de datasets do ano solicitado.

    Retorna None se o ano não estiver disponível.
    """
    try:
        r = requests.get(listing_url, headers=_HEADERS, timeout=20, verify=False)
        r.raise_for_status()

        # HTML usa &amp; em vez de & nos atributos href
        html = r.text.replace("&amp;", "&")

        pattern = rf'href="([^"]*year={year}0[^"]*)"'
        match = re.search(pattern, html)
        if match:
            url = match.group(1)
            if url.startswith("/"):
                url = f"https://www.e-stat.go.jp{url}"
            return url
    except Exception:
        pass
    return None


def _discover_stat_inf_ids(year_url: str) -> List[Dict[str, str]]:
    """
    Na página de datasets de um ano específico, extrai todos os
    statInfId (usados para download de CSV) e títulos das tabelas.

    Retorna lista de dicts com chaves: statInfId, title, csv_url.
    """
    results = []
    try:
        r = requests.get(year_url, headers=_HEADERS, timeout=20, verify=False)
        r.raise_for_status()

        # Decodificar HTML entities nos hrefs
        html = r.text.replace("&amp;", "&")

        csv_links = re.findall(
            r"file-download\?statInfId=(\d+)&fileKind=(\d+)", html
        )
        titles = re.findall(
            r"(\d{4}[.\s]+Commodity by Country[^<\"]{5,80})", html
        )

        for i, (stat_inf_id, file_kind) in enumerate(csv_links):
            if file_kind == "1":
                results.append({
                    "statInfId": stat_inf_id,
                    "title": titles[i].strip() if i < len(titles) else f"Section {i+1}",
                    "csv_url": (
                        f"{_ESTAT_BASE}/stat-search/file-download"
                        f"?statInfId={stat_inf_id}&fileKind=1"
                    ),
                })
    except Exception:
        pass
    return results


def _download_csv_bytes(csv_url: str, max_retries: int = 3) -> Optional[bytes]:
    """Baixa o CSV (ou ZIP contendo CSV) e retorna os bytes brutos."""
    import zipfile
    import io

    for attempt in range(max_retries):
        try:
            r = requests.get(
                csv_url, headers=_HEADERS, timeout=60,
                verify=False, stream=True,
            )
            if r.status_code != 200:
                time.sleep(2 * (attempt + 1))
                continue

            content = r.content
            ct = r.headers.get("Content-Type", "")

            # Pode vir como ZIP
            if "zip" in ct.lower() or content[:2] == b"PK":
                with zipfile.ZipFile(io.BytesIO(content)) as zf:
                    csv_names = [n for n in zf.namelist() if n.lower().endswith(".csv")]
                    if csv_names:
                        return zf.read(csv_names[0])

            return content

        except requests.exceptions.Timeout:
            time.sleep(5)
        except Exception:
            time.sleep(2)
    return None


def _wide_to_long(
    df: pd.DataFrame,
    year: int,
    country_map: Dict[int, str],
    import_export: int,
) -> pd.DataFrame:
    """
    Converte um DataFrame no formato wide do e-stat (Value-Jan…Dec) para
    o formato long com uma linha por (HS, Country, Mês).

    Usa operações vetorizadas (pd.melt) para máxima performance, evitando
    iteração linha a linha.

    Args:
        df: DataFrame bruto lido do CSV do e-stat.
        year: Ano dos dados.
        country_map: Dicionário código_país → nome_país.
        import_export: 1 = Import, 0 = Export.

    Returns:
        DataFrame long com colunas:
        Data, ncm, ImportExport, pais_id, pais_name, peso, valor, frete, seguro
    """
    df = df.copy()

    # Normalizar HS e filtrar confidenciais
    df["HS"] = df["HS"].astype(str).str.replace("'", "").str.strip()
    df["ncm"] = pd.to_numeric(df["HS"].str[:8], errors="coerce")
    df = df.dropna(subset=["ncm"])
    df["ncm"] = df["ncm"].astype(int)

    df["Country"] = pd.to_numeric(df["Country"], errors="coerce").astype("Int64")

    # Determinar coluna de peso (KG): Quantity2 se Unit2='KG', senão Quantity1
    peso_cols = {}
    for month_name, month_num in _MONTH_MAP.items():
        unit2 = df.get("Unit2", pd.Series(dtype=str))
        has_kg_unit2 = unit2.astype(str).str.strip() == "KG"
        q2_col = f"Quantity2-{month_name}"
        q1_col = f"Quantity1-{month_name}"
        # Criar coluna peso unificada para cada mês
        peso_col_name = f"peso_{month_name}"
        if q2_col in df.columns and q1_col in df.columns:
            df[peso_col_name] = pd.to_numeric(df[q2_col], errors="coerce").fillna(0)
            df.loc[~has_kg_unit2, peso_col_name] = pd.to_numeric(
                df.loc[~has_kg_unit2, q1_col], errors="coerce"
            ).fillna(0)
        elif q1_col in df.columns:
            df[peso_col_name] = pd.to_numeric(df[q1_col], errors="coerce").fillna(0)
        else:
            df[peso_col_name] = 0
        peso_cols[month_name] = peso_col_name

    # Melt dos valores (Value-XXX)
    value_cols = {m: f"Value-{m}" for m in _MONTH_MAP if f"Value-{m}" in df.columns}
    id_vars = ["ncm", "HS", "Country"]

    # Melt valor
    df_val = df[id_vars + list(value_cols.values())].copy()
    df_val = df_val.melt(
        id_vars=id_vars,
        value_vars=list(value_cols.values()),
        var_name="month_raw",
        value_name="valor",
    )
    df_val["month_name"] = df_val["month_raw"].str.replace("Value-", "")
    df_val["valor"] = pd.to_numeric(df_val["valor"], errors="coerce").fillna(0)

    # Melt peso
    df_peso = df[id_vars + list(peso_cols.values())].copy()
    df_peso = df_peso.melt(
        id_vars=id_vars,
        value_vars=list(peso_cols.values()),
        var_name="month_raw",
        value_name="peso",
    )
    df_peso["month_name"] = df_peso["month_raw"].str.replace("peso_", "")
    df_peso["peso"] = pd.to_numeric(df_peso["peso"], errors="coerce").fillna(0)

    # Juntar valor e peso
    merge_keys = id_vars + ["month_name"]
    result = df_val[merge_keys + ["valor"]].merge(
        df_peso[merge_keys + ["peso"]],
        on=merge_keys,
        how="outer",
    )

    # Construir coluna Data
    result["month_num"] = result["month_name"].map(_MONTH_MAP)
    result["Data"] = pd.to_datetime(
        year * 10000 + result["month_num"].astype(int) * 100 + 1,
        format="%Y%m%d",
    )

    # Mapear país
    result["pais_id"] = result["Country"]
    result["pais_name"] = result["Country"].map(country_map).fillna("UNKNOWN")

    # Flags e colunas padrão
    result["ImportExport"] = import_export
    result["frete"] = np.nan
    result["seguro"] = np.nan

    # Selecionar e retornar
    cols_final = [
        "Data", "ncm", "ImportExport", "pais_id", "pais_name",
        "peso", "valor", "frete", "seguro",
    ]
    return result[cols_final].reset_index(drop=True)


def _load_country_map(paises_file: Path) -> Dict[int, str]:
    """Carrega planilha de códigos de país → nome."""
    df = pd.read_excel(paises_file)
    cod_col = df.columns[1]
    pais_col = df.columns[2]
    df = df[[cod_col, pais_col]].dropna(subset=[cod_col])
    df[cod_col] = df[cod_col].astype(int)
    return dict(zip(df[cod_col], df[pais_col]))


# ──────────────────────────────────────────────────────────────────
# Classe principal
# ──────────────────────────────────────────────────────────────────

class COMEX_JPN_NM(ComexPipelineNM):
    """
    Pipeline de COMEX específico para o Japão (JPN) usando Nova Metodologia.

    Coleta dados diretamente do portal e-stat, sem necessidade de API key.
    Os dados são anuais, divididos em 22 seções (chapters) por tipo
    (Import/Export), cada uma contendo valores mensais em formato wide.

    Semelhanças com os demais NM:
    - Herda ComexPipelineNM (mesma classe base que BRA, ITA, DEU, etc.)
    - Usa o mesmo data-contract.yaml para normalização de colunas
    - Segue o fluxo collect → update_historical → normalize → calculate → upload
    - Utiliza IDS_comex.xlsx para vincular NCM × ImportExport → IDIndicePrincipal

    Diferenças em relação aos EUR (ITA, DEU, NLD, …):
    - Faz coleta própria (como BRA), pois não vem do histórico compartilhado EUR
    - Fonte é e-stat.go.jp e não Eurostat/comex.gov.br
    - Dados em Yen (¥), sem frete/seguro desagregados

    Diferenças em relação ao BRA:
    - Fonte diferente (e-stat vs gov.br)
    - Formato wide → precisa de unpivot para long
    - HS de 9 dígitos → trunca para 8 (NCM)
    - Sem frete/seguro (como os EUR)
    """

    # Caminho da planilha de códigos de país (relativo à raiz NM/)
    _PAISES_XLSX_GLOB = "*digo*Pa*s*.xlsx"

    def __init__(self, start_date=None, use_azure=True, developing=False):
        super().__init__(
            iso_code="JPN",
            start_date=start_date,
            data_contract_path="data-contract.yaml",
            ids_table_path="IDS_comex.xlsx",
            use_azure=use_azure,
            developing=developing,
        )

        # Resolver planilha de países
        nm_dir = Path(__file__).parent.parent.parent  # NM/
        matches = list(nm_dir.glob(self._PAISES_XLSX_GLOB))
        if not matches:
            self.logger.warning(
                f"Planilha de códigos de países não encontrada em {nm_dir}. "
                "O mapeamento pais_id → pais_name ficará incompleto."
            )
            self._country_map: Dict[int, str] = {}
        else:
            self._country_map = _load_country_map(matches[0])
            self.logger.info(
                f"Planilha de países carregada: {len(self._country_map)} países"
            )

    # ─── Métodos obrigatórios do framework ──────────────────────

    def _get_country_name(self) -> str:
        """Retorna nome do país para filtro na tabela de IDs."""
        return "Japão"

    # ─── Coleta ─────────────────────────────────────────────────

    def _collect_trade_type(self, tclass2: str, import_export: int) -> pd.DataFrame:
        """
        Coleta todas as seções de um tipo de comércio (Import ou Export)
        para o período definido em self.start_date até hoje.

        Fluxo:
        1. Resolve a URL de listagem (dinamicamente, a partir da página oficial)
        2. Para cada ano no período, descobre a URL do ano
        3. Descobre os statInfId de cada seção
        4. Baixa cada CSV e transforma de wide para long
        5. Concatena e retorna

        Args:
            tclass2: Código e-stat do tipo (Import ou Export).
            import_export: 1 = Import, 0 = Export.

        Returns:
            DataFrame no schema padrão NM.
        """
        trade_label = "Import" if import_export == 1 else "Export"
        self.logger.info(f"[{trade_label}] Iniciando coleta...")

        # Determinar anos a coletar
        start_year = self.start_date.year
        end_year = datetime.now().year
        years = list(range(start_year, end_year + 1))
        self.logger.info(f"[{trade_label}] Anos a coletar: {years}")

        # Resolver URL de listagem dinâmica
        listing_url = _resolve_listing_url(tclass2)
        self.logger.info(f"[{trade_label}] URL de listagem: {listing_url[:100]}...")

        all_dfs = []

        for year in years:
            self.logger.info(f"[{trade_label}][{year}] Descobrindo datasets...")
            year_url = _discover_year_url(listing_url, year)

            if not year_url:
                self.logger.warning(
                    f"[{trade_label}][{year}] Ano não encontrado na listagem. Pulando."
                )
                continue

            tables = _discover_stat_inf_ids(year_url)
            self.logger.info(
                f"[{trade_label}][{year}] {len(tables)} seções encontradas"
            )

            for i, tbl in enumerate(tables, 1):
                csv_bytes = _download_csv_bytes(tbl["csv_url"])
                if csv_bytes is None:
                    self.logger.warning(
                        f"[{trade_label}][{year}][{i:02d}] "
                        f"Falha ao baixar: {tbl['title'][:50]}"
                    )
                    continue

                try:
                    df_raw = pd.read_csv(
                        BytesIO(csv_bytes),
                        encoding="utf-8",
                        sep=None,
                        engine="python",
                        dtype=str,
                    )
                except Exception as e:
                    self.logger.warning(
                        f"[{trade_label}][{year}][{i:02d}] "
                        f"Erro ao ler CSV: {e}"
                    )
                    continue

                df_long = _wide_to_long(
                    df_raw, year, self._country_map, import_export
                )

                # Filtrar linhas sem dados (valor e peso == 0)
                df_long = df_long[
                    (df_long["valor"] > 0) | (df_long["peso"] > 0)
                ].copy()

                if not df_long.empty:
                    all_dfs.append(df_long)
                    self.logger.info(
                        f"[{trade_label}][{year}][{i:02d}] "
                        f"{len(df_long):,} registros | {tbl['title'][:45]}"
                    )

                # Delay entre requests
                time.sleep(0.8)

        if not all_dfs:
            self.logger.warning(f"[{trade_label}] Nenhum dado coletado")
            return pd.DataFrame()

        result = pd.concat(all_dfs, ignore_index=True)

        # Filtrar por start_date
        result = result[result["Data"] >= pd.Timestamp(self.start_date)].copy()

        self.logger.info(
            f"[{trade_label}] Total coletado: {len(result):,} registros | "
            f"{result['ncm'].nunique():,} NCMs | "
            f"{result['pais_name'].nunique():,} países"
        )
        return result

    def collect_import_data(self) -> pd.DataFrame:
        """Coleta dados de importação do Japão via e-stat."""
        return self._collect_trade_type(_TCLASS2_IMPORT, import_export=1)

    def collect_export_data(self) -> pd.DataFrame:
        """Coleta dados de exportação do Japão via e-stat."""
        return self._collect_trade_type(_TCLASS2_EXPORT, import_export=0)

    # ─── Tratamento específico ──────────────────────────────────

    def _country_specific_treatment(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Tratamento específico para dados do Japão.

        Garante que:
        - Coluna Data existe e está em datetime
        - ncm é int
        - ImportExport é int (1=Import, 0=Export)
        - frete e seguro existem (NaN, pois e-stat não fornece)

        Args:
            df: DataFrame após normalização de colunas.

        Returns:
            DataFrame tratado, pronto para as fases seguintes.
        """
        df = df.copy()

        # Garantir coluna Data
        if "Data" not in df.columns:
            self.logger.warning("Coluna Data não encontrada após normalização")
            return df

        df["Data"] = pd.to_datetime(df["Data"], errors="coerce")

        # Garantir tipos numéricos
        if "ncm" in df.columns:
            df["ncm"] = pd.to_numeric(df["ncm"], errors="coerce")
            df = df.dropna(subset=["ncm"])
            df["ncm"] = df["ncm"].astype(int)

        if "valor" in df.columns:
            df["valor"] = pd.to_numeric(df["valor"], errors="coerce").fillna(0)

        if "peso" in df.columns:
            df["peso"] = pd.to_numeric(df["peso"], errors="coerce").fillna(0)

        if "ImportExport" in df.columns:
            df["ImportExport"] = pd.to_numeric(
                df["ImportExport"], errors="coerce"
            ).astype(int)

        # Garantir frete e seguro
        if "frete" not in df.columns:
            df["frete"] = np.nan
        if "seguro" not in df.columns:
            df["seguro"] = np.nan

        return df


# ──────────────────────────────────────────────────────────────────
# Execução local
# ──────────────────────────────────────────────────────────────────

def main():
    """Execução local do pipeline completo."""
    today = datetime.now()
    months_lookback = 5
    start = (today.replace(day=1) - pd.DateOffset(months=months_lookback)).to_pydatetime()

    pipeline = COMEX_JPN_NM(start_date=start, developing=True)

    result_df = pipeline.run()

    return result_df


if __name__ == "__main__":
    result = main()
    print(f"\nPipeline concluído! Total de registros: {len(result)}")
