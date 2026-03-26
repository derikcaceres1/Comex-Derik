import warnings
import sys
import os
from pathlib import Path
import pandas as pd
from typing import List, Dict
from datetime import datetime
from io import StringIO
import calendar
import re
import requests
from bs4 import BeautifulSoup
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from requests.exceptions import RequestException, ConnectionError, Timeout

# Adicionar path do diretório OM ao sys.path para imports
project_root = Path(__file__).parent.parent.parent
om_path = project_root / "OM"
if str(om_path) not in sys.path:
    sys.path.insert(0, str(om_path))

from OM.costdrivers_comex_OM import ComexPipeline

# Disable warnings globally
warnings.filterwarnings("ignore")


class COMEX_IND(ComexPipeline):
    """Pipeline de COMEX específico para Índia."""
    
    def __init__(self, start_date=None, use_azure=True):
        super().__init__(
            iso_code='IND',
            start_date=start_date,
            data_contract_path='data-contract.yaml',
            ids_table_path='IDS_comex.xlsx',
            use_azure=use_azure
        )
        
        # Headers HTTP do código antigo
        self.headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Accept-Language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
            'Cache-Control': 'max-age=0',
            'Connection': 'keep-alive',
            'Content-Type': 'application/x-www-form-urlencoded',
            'Origin': 'https://tradestat.commerce.gov.in',
            'Referer': 'https://tradestat.commerce.gov.in/',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'same-origin',
            'Sec-Fetch-User': '?1',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36',
            'sec-ch-ua': '"Google Chrome";v="141", "Not?A_Brand";v="8", "Chromium";v="141"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
        }
        
        # Criar sessão requests para reutilização
        self.session = requests.Session()
        self.session.headers.update(self.headers)
    
    def _get_country_name(self) -> str:
        """Retorna nome do país para filtro na tabela de IDs."""
        return 'Índia'
    
    @retry(
        retry=retry_if_exception_type((RequestException, ConnectionError, Timeout)),
        wait=wait_exponential(multiplier=1, min=1, max=8),
        stop=stop_after_attempt(3),
        reraise=True,
        before_sleep=lambda retry_state: print(
            f"Falha {retry_state.attempt_number}/3 — tentando novamente..."
        )
    )
    def safe_request(self, s, method, url, **kwargs):
        """
        Wrapper resiliente para GET/POST com tentativas automáticas.
        """
        if method.lower() == "get":
            r = s.get(url, timeout=60, **kwargs)
        else:
            r = s.post(url, timeout=60, **kwargs)
        
        r.raise_for_status()
        return r
    
    def list_available_countries(self, s, tipo, prefix, mes, ano):
        """
        Usa o token e o data já preparados para fazer o POST e retornar {codigo: país}.
        """
        url_country = f"https://tradestat.commerce.gov.in/meidb/country_wise_{tipo}"
        r = self.safe_request(s, "get", url_country)
        soup = BeautifulSoup(r.text, "html.parser")
        token = soup.find("input", {"name": "_token"})["value"]
        
        data_country = {
            "_token": token,
            f"{prefix}Month": str(mes),
            f"{prefix}Year": str(ano),
            f"{prefix}Country": "all",
            f"{prefix}ReportVal": "1",
            f"{prefix}ReportYear": "2",
        }
        
        resp = self.safe_request(s, "post", url_country, data=data_country)
        soup = BeautifulSoup(resp.text, "html.parser")
        table = soup.find("table")
        
        select = soup.find("select", {"name": f"{prefix}Country"})
        country_map = {
            opt.get("value"): opt.text.strip()
            for opt in select.find_all("option")
            if opt.get("value") not in [None, "all"]
        }
        
        df = pd.read_html(StringIO(str(table)))[0]
        col_pais = next((c for c in df.columns if "Country" in c), None)
        col_valor = next((c for c in df.columns if any(m in c for m in calendar.month_abbr[1:])), None)
        
        df[col_valor] = (
            df[col_valor].astype(str)
            .str.replace(",", "", regex=False)
            .replace("-", "0")
            .replace("-", "0")
            .astype(float)
            .fillna(0)
        )
        
        df_filtrado = df[df[col_valor] > 0]
        country_map_lower = {v.lower(): k for k, v in country_map.items()}
        return {
            country_map_lower[nome.lower()]: nome
            for nome in df_filtrado[col_pais]
            if nome.lower() in country_map_lower
        }
    
    def validate_html_has_table(self, html_text: str) -> bool:
        """
        Valida se o HTML contém uma tabela válida antes de processar.
        
        Args:
            html_text: Texto HTML da resposta
            
        Returns:
            True se contém tabela válida, False caso contrário
        """
        if not html_text or len(html_text) < 100:
            return False
        
        try:
            soup = BeautifulSoup(html_text, "html.parser")
            table = soup.find("table")
            if not table:
                return False
            
            # Verificar se tem dados (linhas além do cabeçalho)
            rows = table.find_all("tr")
            return len(rows) > 1
        except Exception:
            return False
    
    def safe_read_html(self, html_text: str, country_name: str = "") -> pd.DataFrame:
        """
        Lê HTML de forma segura, validando se contém tabela antes de processar.
        
        Args:
            html_text: Texto HTML da resposta
            country_name: Nome do país para logging
            
        Returns:
            DataFrame com os dados ou DataFrame vazio se não houver tabela válida
            
        Raises:
            ValueError: Se não houver tabela válida no HTML
        """
        if not self.validate_html_has_table(html_text):
            error_msg = f"Nenhuma tabela válida encontrada no HTML"
            if country_name:
                error_msg += f" para país {country_name}"
            self.logger.warning(error_msg)
            raise ValueError(error_msg)
        
        try:
            tables = pd.read_html(StringIO(html_text))
            if not tables or len(tables) == 0:
                error_msg = f"pd.read_html não retornou tabelas"
                if country_name:
                    error_msg += f" para país {country_name}"
                self.logger.warning(error_msg)
                raise ValueError(error_msg)
            return tables[0]
        except ValueError as e:
            if "No tables found" in str(e):
                error_msg = f"Nenhuma tabela encontrada no HTML"
                if country_name:
                    error_msg += f" para país {country_name}"
                self.logger.warning(error_msg)
            raise
    
    def treat_dataframe(self, df, pais_nome, tipo, value_name):
        """
        Padroniza e transforma o DataFrame de valores/peso para formato longo.
        Retorna DataFrame com colunas: ["NCM", value_name, "Data", "Tipo", "Pais"]
        """
        chave = "HSCode" if "HSCode" in df.columns else df.columns[1]
        df.rename(columns={chave: "NCM"}, inplace=True)
        df.drop(columns=df.filter(regex=r'([A-Z][a-z]{2})-\1').columns, inplace=True)
        
        col_datas = [c for c in df.columns if re.search(r"[A-Za-z]{3}-\d{4}", c)]
        
        df_long = df.melt(
            id_vars=["NCM"],
            value_vars=col_datas,
            var_name="DataRaw",
            value_name=value_name,
        )
        
        df_long["Data"] = pd.to_datetime(
            df_long["DataRaw"].str.extract(r"([A-Za-z]{3}-\d{4})")[0],
            format="%b-%Y"
        )
        df_long["Data"] = pd.to_datetime(df_long["Data"])
        df_long["Data"] = df_long["Data"].dt.to_period("M").apply(lambda p: p.to_timestamp())
        df_long["Data"] = df_long["Data"].dt.strftime("%Y-%m-%d")
        
        df_long[value_name] = (
            df_long[value_name]
            .astype(str)
            .replace("-", "0")
            .replace("-", "0")
            .astype(float)
        )
        
        df_long["Tipo"] = tipo
        df_long["Pais"] = pais_nome
        return df_long[["NCM", value_name, "Data", "Tipo", "Pais"]]
    
    def export_data(self, s, mes, ano, tipo, url):
        """Coleta dados de exportação para um mês/ano específico."""
        r = self.safe_request(s, "get", url)
        soup = BeautifulSoup(r.text, "html.parser")
        _token = soup.find("input", {"name": "_token"})["value"]
        prefix = "cwe"
        
        countries = self.list_available_countries(s, tipo, prefix, mes, ano)
        self.logger.info(f"{len(countries)} códigos coletados ({tipo.upper()})")
        
        all_dfs = []
        total = len(countries)
        for i, (pid, nome) in enumerate(countries.items(), start=1):
            self.logger.info(f"Coletando país {i}/{total} — {nome} ({tipo.upper()})...")
            
            try:
                # Peso (Quantity)
                data_peso = {
                    "_token": _token,
                    "cwcexddMonth": str(mes),
                    "cwcexddYear": str(ano),
                    "cwcexallcount": pid,
                    "cwcexddCommodityLevel": "8",
                    "cwcexddReportVal": "2",
                    "cwcexddReportYear": "2",
                }
                resp_peso = self.safe_request(s, "post", url, data=data_peso)
                df_peso = self.safe_read_html(resp_peso.text, country_name=nome)
                df_peso = self.treat_dataframe(df_peso, nome, tipo, "Peso")
                df_peso["NCM"] = df_peso["NCM"].astype(str).str.strip()
                
                # Valor (USD)
                data_valor = {
                    "_token": _token,
                    "cwcexddMonth": str(mes),
                    "cwcexddYear": str(ano),
                    "cwcexallcount": pid,
                    "cwcexddCommodityLevel": "8",
                    "cwcexddReportVal": "1",
                    "cwcexddReportYear": "2",
                }
                resp_valor = self.safe_request(s, "post", url, data=data_valor)
                df_valor = self.safe_read_html(resp_valor.text, country_name=nome)
                df_valor = self.treat_dataframe(df_valor, nome, tipo, "Valor")
                df_valor["NCM"] = df_valor["NCM"].astype(str).str.strip()
                
                df = pd.merge(df_valor, df_peso, on=["NCM", "Data", "Tipo", "Pais"])
                all_dfs.append(df)
            except (ValueError, Exception) as e:
                self.logger.warning(f"Erro ao processar país {nome} ({tipo.upper()}): {e}. Pulando...")
                continue
        
        if not all_dfs:
            return pd.DataFrame()
        
        df_final = pd.concat(all_dfs, ignore_index=True)
        self.logger.info(f"Export data collected: {len(df_final)} rows.")
        return df_final[["NCM", "Valor", "Peso", "Data", "Tipo", "Pais"]]
    
    def import_data(self, s, mes, ano, tipo, url):
        """Coleta dados de importação para um mês/ano específico."""
        r = self.safe_request(s, "get", url)
        soup = BeautifulSoup(r.text, "html.parser")
        _token = soup.find("input", {"name": "_token"})["value"]
        prefix = "cwim"
        
        countries = self.list_available_countries(s, tipo, prefix, mes, ano)
        self.logger.info(f"{len(countries)} códigos coletados ({tipo.upper()})")
        
        all_dfs = []
        total = len(countries)
        for i, (pid, nome) in enumerate(countries.items(), start=1):
            self.logger.info(f"Coletando país {i}/{total} — {nome} ({tipo.upper()})...")
            
            try:
                data_peso = {
                    "_token": _token,
                    "cwcimMonth": str(mes),
                    "cwcimYear": str(ano),
                    "cwcimallcount": pid,
                    "cwcimCommodityLevel": "8",
                    "cwcimReportVal": "2",
                    "cwcimReportYear": "2",
                }
                resp_peso = self.safe_request(s, "post", url, data=data_peso)
                df_peso = self.safe_read_html(resp_peso.text, country_name=nome)
                df_peso = self.treat_dataframe(df_peso, nome, tipo, "Peso")
                df_peso["NCM"] = df_peso["NCM"].astype(str).str.strip()
                
                data_valor = {
                    "_token": _token,
                    "cwcimMonth": str(mes),
                    "cwcimYear": str(ano),
                    "cwcimallcount": pid,
                    "cwcimCommodityLevel": "8",
                    "cwcimReportVal": "1",
                    "cwcimReportYear": "2",
                }
                
                resp_valor = self.safe_request(s, "post", url, data=data_valor)
                df_valor = self.safe_read_html(resp_valor.text, country_name=nome)
                df_valor = self.treat_dataframe(df_valor, nome, tipo, "Valor")
                df_valor["NCM"] = df_valor["NCM"].astype(str).str.strip()
                
                df = pd.merge(df_valor, df_peso, on=["NCM", "Data", "Tipo", "Pais"])
                all_dfs.append(df)
            except (ValueError, Exception) as e:
                self.logger.warning(f"Erro ao processar país {nome} ({tipo.upper()}): {e}. Pulando...")
                continue
        
        if not all_dfs:
            return pd.DataFrame()
        
        df_final = pd.concat(all_dfs, ignore_index=True)
        return df_final[["NCM", "Valor", "Peso", "Data", "Tipo", "Pais"]]
    
    def collect_import_data(self) -> pd.DataFrame:
        """Coleta dados de importação da Índia desde 2020-01-01."""
        self.logger.info("Coletando dados de importação...")
        
        # Coletar desde 2023-01-01 até hoje
        data_inicial = "2023-01-01"
        data_final = datetime.now().strftime("%Y-%m-%d")
        date_range = pd.date_range(start=data_inicial, end=data_final, freq="MS").to_period("M")
        
        all_dfs = []
        for date in date_range:
            mes, ano = date.month, date.year
            url = f"https://tradestat.commerce.gov.in/meidb/country_wise_all_commodities_import"
            self.logger.info(f"Coletando mês {mes:02d}/{ano} para o IMPORT...")
            try:
                df_tipo = self.import_data(self.session, mes, ano, "import", url)
                if df_tipo is not None and not df_tipo.empty:
                    df_tipo['ImportExport'] = 1
                    all_dfs.append(df_tipo)
                    self.logger.info(f"IMPORT {mes}/{ano}: {len(df_tipo)} linhas coletadas.")
                else:
                    self.logger.warning(f"Nenhum dado para IMPORT {mes}/{ano}.")
            except Exception as e:
                self.logger.error(f"Erro ao coletar import {mes}/{ano}: {e}")
        
        if all_dfs:
            final_df = pd.concat(all_dfs, ignore_index=True)
            self.logger.info(f"Coleta de importação finalizada! Total de linhas: {len(final_df)}")
            return final_df
        else:
            self.logger.warning("Nenhum dado de importação foi coletado.")
            return pd.DataFrame()
    
    def collect_export_data(self) -> pd.DataFrame:
        """Coleta dados de exportação da Índia desde 2020-01-01."""
        self.logger.info("Coletando dados de exportação...")
        
        # Coletar desde 2020-01-01 até hoje
        data_inicial = "2023-01-01"
        data_final = datetime.now().strftime("%Y-%m-%d")
        date_range = pd.date_range(start=data_inicial, end=data_final, freq="MS").to_period("M")
        
        all_dfs = []
        for date in date_range:
            mes, ano = date.month, date.year
            url = f"https://tradestat.commerce.gov.in/meidb/country_wise_all_commodities_export"
            self.logger.info(f"Coletando mês {mes:02d}/{ano} para o EXPORT...")
            try:
                df_tipo = self.export_data(self.session, mes, ano, "export", url)
                if df_tipo is not None and not df_tipo.empty:
                    df_tipo['ImportExport'] = 0
                    all_dfs.append(df_tipo)
                    self.logger.info(f"EXPORT {mes}/{ano}: {len(df_tipo)} linhas coletadas.")
                else:
                    self.logger.warning(f"Nenhum dado para EXPORT {mes}/{ano}.")
            except Exception as e:
                self.logger.error(f"Erro ao coletar export {mes}/{ano}: {e}")
        
        if all_dfs:
            final_df = pd.concat(all_dfs, ignore_index=True)
            self.logger.info(f"Coleta de exportação finalizada! Total de linhas: {len(final_df)}")
            return final_df
        else:
            self.logger.warning("Nenhum dado de exportação foi coletado.")
            return pd.DataFrame()
    
    def _country_specific_treatment(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Tratamento específico para dados da Índia.
        
        - Converter formato de data para datetime
        - Limpar valores numéricos
        - Garantir que NCM seja string e trim
        - Renomear Pais para pais_name após normalização
        """
        if df.empty:
            return df
        
        # Converter Data para datetime se ainda não estiver
        if 'Data' in df.columns:
            df['Data'] = pd.to_datetime(df['Data'], errors='coerce')
            # Remover linhas com data inválida
            df = df[df['Data'].notna()]
        
        # Garantir que NCM seja string e trim
        if 'ncm' in df.columns:
            df['ncm'] = df['ncm'].astype(str).str.strip()
            # Remover NCM nulos ou vazios
            df = df[df['ncm'].notna()]
            df = df[df['ncm'] != '']
        
        # Limpar valores numéricos (remover vírgulas, tratar "-" como 0)
        if 'valor' in df.columns:
            df['valor'] = (
                df['valor'].astype(str)
                .str.replace(",", "", regex=False)
                .replace("-", "0")
                .replace("nan", "0")
                .astype(float)
                .fillna(0)
            )
        
        if 'peso' in df.columns:
            df['peso'] = (
                df['peso'].astype(str)
                .str.replace(",", "", regex=False)
                .replace("-", "0")
                .replace("nan", "0")
                .astype(float)
                .fillna(0)
            )
        
        return df


def main():
    """Execução local do pipeline completo."""
    # Criar instância do pipeline
    pipeline = COMEX_IND(start_date=None)
    
    # Executar pipeline completo
    result_df = pipeline.run()
    
    # Salvar resultado final localmente
    # timestamp = datetime.now().strftime('%Y%m%dT%H%M')
    # result_df.to_excel(f'subir_COMEX_IND_{timestamp}.xlsx', index=False)
    
    return result_df


if __name__ == "__main__":
    result = main()
    print(f"\nPipeline concluído! Total de registros: {len(result)}")

