import io
import time
import re
import sys
import os
from pathlib import Path
from datetime import datetime
from io import BytesIO
from zipfile import ZipFile
from typing import List, Dict
import pandas as pd
import requests
from bs4 import BeautifulSoup
import urllib3

# Adicionar path do diretório OM ao sys.path para imports
project_root = Path(__file__).parent.parent.parent
om_path = project_root / "OM"
if str(om_path) not in sys.path:
    sys.path.insert(0, str(om_path))

from OM.costdrivers_comex_OM import ComexPipeline

# Suprimir avisos de SSL (necessário devido a problemas de certificado no servidor DANE)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class COMEX_COL(ComexPipeline):
    """Pipeline de COMEX específico para Colômbia."""
    
    def __init__(self, start_date=None, use_azure=True):
        super().__init__(
            iso_code='COL',
            start_date=start_date,
            data_contract_path='data-contract.yaml',
            ids_table_path='IDS_comex.xlsx',
            use_azure=use_azure
        )
        # URLs para dados recentes (2025)
        self.export_url = "https://microdatos.dane.gov.co/index.php/catalog/859/get-microdata"
        self.import_url = "https://microdatos.dane.gov.co/index.php/catalog/856/get-microdata"
        
        # URLs para dados históricos (múltiplos anos)
        self.export_historical_url = "https://microdatos.dane.gov.co/index.php/catalog/472/get-microdata"
        self.import_historical_url = "https://microdatos.dane.gov.co/index.php/catalog/473/get-microdata"
        
        # Calcular ano inicial padrão (ano atual - 1)
        if start_date is None:
            self.start_year = datetime.now().year - 1
        elif isinstance(start_date, datetime):
            self.start_year = start_date.year
        elif isinstance(start_date, int):
            self.start_year = start_date
        else:
            # Tentar converter para int (caso seja string com ano)
            try:
                self.start_year = int(start_date)
            except (ValueError, TypeError):
                self.logger.warning(f"Não foi possível converter start_date '{start_date}' para ano. Usando padrão (ano - 1).")
                self.start_year = datetime.now().year - 1
    
    def _get_country_name(self) -> str:
        """Retorna nome do país para filtro na tabela de IDs."""
        return 'Colômbia'
    
    def _get_download_link(self, source_url: str) -> str:
        """
        Obtém o link de download da página HTML (para URLs com um único arquivo).
        
        Args:
            source_url: URL da página com o link de download
            
        Returns:
            Link de download ou None em caso de erro
        """
        try:
            self.logger.info(f"Obtendo link de download de: {source_url}")
            # verify=False necessário devido a problemas de certificado SSL no servidor DANE
            response = requests.get(source_url, timeout=30, verify=False)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'html.parser')
            inputs = soup.find_all('input', type="image")
            
            if not inputs:
                self.logger.error("Nenhum input de imagem encontrado na página")
                return None
            
            # Extrair link do onclick
            onclick = inputs[0].get('onclick', '')
            links = [link.strip() for link in onclick.split("'") if 'download' in link]
            
            if not links:
                self.logger.error("Nenhum link de download encontrado no onclick")
                return None
            
            download_link = links[0]
            self.logger.info(f"Link de download obtido: {download_link}")
            return download_link
            
        except requests.RequestException as e:
            self.logger.error(f"Erro ao acessar página: {str(e)}")
            return None
        except Exception as e:
            self.logger.error(f"Erro ao extrair link de download: {str(e)}")
            return None
    
    def _get_historical_download_links(self, source_url: str, ie_type: str) -> List[Dict[str, str]]:
        """
        Obtém todos os links de download da página HTML histórica.
        Extrai links de arquivos ZIP por ano e filtra apenas os anos necessários.
        
        Args:
            source_url: URL da página com múltiplos links de download
            ie_type: 'import' ou 'export' (para identificar prefixo do arquivo: Impo_ ou Expo_)
            
        Returns:
            Lista de dicionários com {'year': ano, 'link': link_download, 'filename': nome_arquivo}
        """
        try:
            self.logger.info(f"Obtendo links de download históricos de: {source_url}")
            # verify=False necessário devido a problemas de certificado SSL no servidor DANE
            response = requests.get(source_url, timeout=60, verify=False)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Encontrar todos os inputs com type="image"
            inputs = soup.find_all('input', type="image")
            
            if not inputs:
                self.logger.warning("Nenhum input de imagem encontrado na página histórica")
                return []
            
            # Determinar prefixo esperado (Impo_ para import, Expo_ para export)
            prefix = "Impo_" if ie_type == "import" else "Expo_"
            
            # Calcular anos necessários: desde start_year até ano_atual
            current_year = datetime.now().year
            target_years = set(range(self.start_year, current_year + 1))
            
            self.logger.info(f"Buscando arquivos para anos: {sorted(target_years)}")
            
            # Usar dicionário para evitar duplicatas por ano (chave: ano, valor: dict com link e filename)
            download_links_dict = {}
            
            for input_elem in inputs:
                onclick = input_elem.get('onclick', '')
                title = input_elem.get('title', '')
                
                # Extrair link do onclick
                links = [link.strip() for link in onclick.split("'") if 'download' in link]
                
                if not links:
                    continue
                
                download_link = links[0]
                
                # Extrair ano do título do arquivo (ex: "Impo_2023.zip" -> 2023)
                year = None
                
                # Tentar extrair do título
                if title:
                    # Procurar padrão como "Impo_2023.zip" ou "Expo_2024.zip"
                    match = re.search(rf'{prefix}(\d{{4}})', title)
                    if match:
                        year = int(match.group(1))
                
                # Se não encontrou no título, tentar no onclick
                if year is None:
                    match = re.search(rf'{prefix}(\d{{4}})', onclick)
                    if match:
                        year = int(match.group(1))
                
                # Se ainda não encontrou, tentar buscar no texto do span próximo
                if year is None:
                    # Procurar pelo span com resource-info que contém o nome do arquivo
                    parent = input_elem.find_parent()
                    if parent:
                        span = parent.find('span', class_='resource-info')
                        if span:
                            span_text = span.get_text(strip=True)
                            match = re.search(rf'{prefix}(\d{{4}})', span_text)
                            if match:
                                year = int(match.group(1))
                
                # Verificar se o ano está nos anos alvo e ainda não foi adicionado
                if year and year in target_years and year not in download_links_dict:
                    download_links_dict[year] = {
                        'year': year,
                        'link': download_link,
                        'filename': title if title else f"{prefix}{year}.zip"
                    }
                    self.logger.info(f"Link encontrado para {prefix}{year}: {download_link}")
            
            # Converter dicionário para lista
            download_links = list(download_links_dict.values())
            self.logger.info(f"Total de {len(download_links)} links históricos encontrados (sem duplicatas)")
            return download_links
            
        except requests.RequestException as e:
            self.logger.error(f"Erro ao acessar página histórica: {str(e)}")
            return []
        except Exception as e:
            self.logger.error(f"Erro ao extrair links de download históricos: {str(e)}")
            return []
    
    def _read_csv_file(self, file) -> pd.DataFrame:
        """
        Lê um arquivo CSV e cria coluna Data.
        
        Args:
            file: Arquivo CSV (file-like object)
            
        Returns:
            DataFrame processado
        """
        df = pd.read_csv(file, encoding="iso-8859-1", sep=";", low_memory=False, dtype=str)
        df.columns = df.columns.str.strip()  # Remove espaços em branco dos nomes das colunas
        
        # Criar coluna Data a partir da primeira coluna (formato '%y%m')
        if len(df.columns) > 0:
            df['Data'] = df[df.columns[0]]
        
        return df
    
    def _read_files_from_link(self, link: str) -> pd.DataFrame:
        """
        Baixa e processa arquivos ZIP aninhados de um link.
        
        Args:
            link: Link de download do ZIP
            
        Returns:
            DataFrame com todos os dados processados
        """
        try:
            self.logger.info(f"Baixando e processando arquivos de: {link}")
            # verify=False necessário devido a problemas de certificado SSL no servidor DANE
            response = requests.get(link, stream=True, timeout=120, verify=False)
            response.raise_for_status()
            
            list_df = []
            
            with ZipFile(BytesIO(response.content)) as z:
                for zip_info in z.infolist():
                    if zip_info.filename.endswith(".zip"):
                        # ZIP aninhado
                        with z.open(zip_info) as zip_file:
                            with ZipFile(BytesIO(zip_file.read())) as inner_zip:
                                for inner_zip_info in inner_zip.infolist():
                                    if inner_zip_info.filename.endswith(".csv"):
                                        self.logger.info(f"Lendo arquivo CSV aninhado: {inner_zip_info.filename}")
                                        with inner_zip.open(inner_zip_info) as csv_file:
                                            df = self._read_csv_file(csv_file)
                                            list_df.append(df)
                    elif zip_info.filename.endswith(".csv"):
                        # CSV direto no ZIP
                        self.logger.info(f"Lendo arquivo CSV: {zip_info.filename}")
                        with z.open(zip_info) as csv_file:
                            df = self._read_csv_file(csv_file)
                            list_df.append(df)
            
            if not list_df:
                self.logger.warning("Nenhum arquivo CSV encontrado no ZIP")
                return pd.DataFrame()
            
            # Concatenar todos os DataFrames
            df = pd.concat(list_df, ignore_index=True)
            
            self.logger.info(f"Total de registros processados: {len(df)}")
            return df
            
        except requests.RequestException as e:
            self.logger.error(f"Erro ao baixar arquivo: {str(e)}")
            return pd.DataFrame()
        except Exception as e:
            self.logger.error(f"Erro ao processar arquivos: {str(e)}")
            return pd.DataFrame()
    
    def collect_import_data(self) -> pd.DataFrame:
        """Coleta dados de importação da Colômbia (recentes + históricos)."""
        self.logger.info("Coletando dados de importação...")
        
        dfs = []
        
        # 1. Coletar dados recentes (ano atual) da URL atual
        self.logger.info("Coletando dados recentes de importação...")
        download_link = self._get_download_link(self.import_url)
        if download_link:
            df_recent = self._read_files_from_link(download_link)
            if not df_recent.empty:
                df_recent['ImportExport'] = 1
                dfs.append(df_recent)
                self.logger.info(f"Dados recentes coletados: {len(df_recent)} registros")
        else:
            self.logger.warning("Não foi possível obter link de download para importação recente")
        
        # 2. Coletar dados históricos desde start_year até ano atual
        self.logger.info(f"Coletando dados históricos de importação desde {self.start_year}...")
        historical_links = self._get_historical_download_links(self.import_historical_url, "import")
        
        for link_info in historical_links:
            year = link_info['year']
            link = link_info['link']
            self.logger.info(f"Processando importação histórica - Ano {year}")
            
            df_historical = self._read_files_from_link(link)
            if not df_historical.empty:
                df_historical['ImportExport'] = 1
                dfs.append(df_historical)
                self.logger.info(f"Dados históricos {year} coletados: {len(df_historical)} registros")
        
        # Concatenar todos os DataFrames
        if not dfs:
            self.logger.warning("Nenhum dado de importação coletado")
            return pd.DataFrame()
        
        result_df = pd.concat(dfs, ignore_index=True)
        self.logger.info(f"Total importação coletado: {len(result_df)} registros")
        
        return result_df
    
    def collect_export_data(self) -> pd.DataFrame:
        """Coleta dados de exportação da Colômbia (recentes + históricos)."""
        self.logger.info("Coletando dados de exportação...")
        
        dfs = []
        
        # 1. Coletar dados recentes (ano atual) da URL atual
        self.logger.info("Coletando dados recentes de exportação...")
        download_link = self._get_download_link(self.export_url)
        if download_link:
            df_recent = self._read_files_from_link(download_link)
            if not df_recent.empty:
                df_recent['ImportExport'] = 0
                dfs.append(df_recent)
                self.logger.info(f"Dados recentes coletados: {len(df_recent)} registros")
        else:
            self.logger.warning("Não foi possível obter link de download para exportação recente")
        
        # 2. Coletar dados históricos desde start_year até ano atual
        self.logger.info(f"Coletando dados históricos de exportação desde {self.start_year}...")
        historical_links = self._get_historical_download_links(self.export_historical_url, "export")
        
        for link_info in historical_links:
            year = link_info['year']
            link = link_info['link']
            self.logger.info(f"Processando exportação histórica - Ano {year}")
            
            df_historical = self._read_files_from_link(link)
            if not df_historical.empty:
                df_historical['ImportExport'] = 0
                dfs.append(df_historical)
                self.logger.info(f"Dados históricos {year} coletados: {len(df_historical)} registros")
        
        # Concatenar todos os DataFrames
        if not dfs:
            self.logger.warning("Nenhum dado de exportação coletado")
            return pd.DataFrame()
        
        result_df = pd.concat(dfs, ignore_index=True)
        self.logger.info(f"Total exportação coletado: {len(result_df)} registros")
        
        return result_df
    
    def _country_specific_treatment(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Tratamento específico para dados da Colômbia.
        
        - Remove linhas com '-' em valor e peso
        - Converte formato numérico (substitui '.' por '' e ',' por '.')
        - Filtra peso > 0
        - Converte Data para datetime (formato '%y%m')
        """
        if df.empty:
            return df
        
        # remover ncm nulo
        if 'ncm' in df.columns:
            df = df[~df['ncm'].isnull()]
        
        # Remover linhas com '-' em valor e peso
        if 'valor' in df.columns:
            df = df[~df['valor'].astype(str).str.contains('-', na=False)]
        if 'peso' in df.columns:
            df = df[~df['peso'].astype(str).str.contains('-', na=False)]
        
        # Converter formato numérico
        if 'valor' in df.columns:
            df['valor'] = df['valor'].astype(str).apply(
                lambda x: str(x).replace('.', '').replace(',', '.').strip()
            ).astype(float)
        
        if 'peso' in df.columns:
            df['peso'] = df['peso'].astype(str).apply(
                lambda x: str(x).replace('.', '').replace(',', '.')
            ).astype(float)
        
        # Converter Data para datetime (formato '%y%m')
        if 'Data' in df.columns:
            df['Data'] = pd.to_datetime(df['Data'], format='%y%m', errors='coerce')
            # Remover linhas com data inválida
            df = df[df['Data'].notna()]
        
        return df


def main():
    """Execução local do pipeline completo."""
    # Criar instância do pipeline
    pipeline = COMEX_COL(start_date=None)
    
    # Executar pipeline completo
    result_df = pipeline.run()
    
    # Salvar resultado final localmente
    # timestamp = datetime.now().strftime('%Y%m%dT%H%M')
    # result_df.to_excel(f'subir_COMEX_COL_{timestamp}.xlsx', index=False)
    
    return result_df


if __name__ == "__main__":
    result = main()
    print(f"\nPipeline concluído! Total de registros: {len(result)}")

