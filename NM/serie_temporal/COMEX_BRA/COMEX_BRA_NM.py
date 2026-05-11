import ssl
from datetime import datetime, timedelta
from io import BytesIO
from typing import List, Dict
import pandas as pd
import requests
from bs4 import BeautifulSoup
import warnings
import sys
import os
from pathlib import Path

# Adicionar path do diretório serie_temporal ao sys.path para imports
project_root = Path(__file__).parent.parent.parent.parent
serie_temporal_path = project_root / "NM" / "serie_temporal"
if str(serie_temporal_path) not in sys.path:
    sys.path.insert(0, str(serie_temporal_path))

from costdrivers_comex_NM import ComexPipelineNM

# Desativar avisos
warnings.filterwarnings("ignore", category=UserWarning, module="bs4")
requests.packages.urllib3.disable_warnings(requests.packages.urllib3.exceptions.InsecureRequestWarning)


class COMEX_BRA_NM(ComexPipelineNM):
    """Pipeline de COMEX específico para Brasil usando Nova Metodologia."""
    
    def __init__(self, start_date=None, use_azure=True, developing=False):
        super().__init__(
            iso_code='BRA',
            start_date=start_date,
            data_contract_path='data-contract.yaml',
            ids_table_path='IDS_comex.xlsx',
            use_azure=use_azure,
            developing=developing
        )
        self.base_url = "https://www.gov.br/produtividade-e-comercio-exterior/pt-br/assuntos/comercio-exterior/estatisticas/base-de-dados-bruta"
        # Configurar SSL não verificado
        ssl._create_default_https_context = ssl._create_unverified_context
    
    def _get_country_name(self) -> str:
        """Retorna nome do país para filtro na tabela de IDs."""
        return 'Brasil'
    
    def _get_available_files(self) -> List[Dict[str, str]]:
        """
        Retorna lista de arquivos CSV disponíveis no portal COMEX.
        
        Returns:
            Lista de dicionários com links e tipos de arquivos encontrados
        """
        try:
            page = requests.get(self.base_url, verify=False, timeout=30)
            page.raise_for_status()
            
            soup = BeautifulSoup(page.content, "html.parser")
            current_year = datetime.now().year
            years_to_search = {str(year) for year in range(self.start_date.year, current_year + 1)}
            
            files = []
            for link in soup.find_all("a"):
                href = link.get("href", "")
                
                if "ncm" in href.lower() and "csv" in href.lower() and any(year in href for year in years_to_search):
                    files.append({
                        "url": href,
                        "ImportExport": "export" if "EXP" in href.upper() else "import",
                        "year": next((year for year in years_to_search if year in href), None)
                    })
            
            self.logger.info(f"Encontrados {len(files)} arquivos no portal COMEX")
            return files
            
        except requests.RequestException as e:
            self.logger.error(f"Erro ao acessar o portal COMEX: {str(e)}")
            return []
    
    def _download_file(self, file_info: Dict[str, str]) -> bytes:
        """
        Baixa um arquivo específico do COMEX.
        
        Args:
            file_info: Dicionário com informações do arquivo (url, ImportExport, year)
            
        Returns:
            Bytes do arquivo CSV baixado, ou None em caso de erro
        """
        try:
            self.logger.info(f"Baixando arquivo: {file_info['url']}")
            response = requests.get(file_info["url"], verify=False, stream=True, timeout=60)
            response.raise_for_status()
            
            self.logger.info(f"Arquivo baixado com sucesso: {file_info['url']}")
            return response.content
            
        except requests.RequestException as e:
            self.logger.error(f"Erro ao baixar arquivo {file_info['url']}: {str(e)}")
            return None
    
    def _process_ie_type(self, ie_type: str) -> pd.DataFrame:
        """
        Processa dados de importação ou exportação.
        
        Args:
            ie_type: 'import' ou 'export'
            
        Returns:
            DataFrame com dados processados
        """
        dfs = []
        files = self._get_available_files()
        
        # Filtrar arquivos do tipo solicitado
        filtered_files = [f for f in files if f['ImportExport'] == ie_type]
        
        if not filtered_files:
            self.logger.warning(f"Nenhum arquivo encontrado para {ie_type}")
            return pd.DataFrame()
        
        for file_info in filtered_files:
            self.logger.info(f"Processando {ie_type.upper()} - Ano {file_info['year']}")
            
            csv_bytes = self._download_file(file_info)
            
            if csv_bytes is None:
                self.logger.warning(f"Erro ao baixar arquivo para {ie_type} no ano {file_info['year']}")
                continue
            
            try:
                # Ler CSV
                df = pd.read_csv(BytesIO(csv_bytes), sep=';', encoding='utf-8', dtype=str)
                
                # Criar coluna Data a partir de CO_MES e CO_ANO
                df['Data'] = pd.to_datetime(
                    df['CO_MES'].astype(str).str.zfill(2) + '-' + df['CO_ANO'].astype(str),
                    format='%m-%Y'
                )
                
                # Adicionar flag ImportExport (será usado depois)
                df['ImportExport'] = 1 if ie_type == 'import' else 0
                
                dfs.append(df)
                self.logger.info(f"DataFrame criado: {len(df)} registros para {ie_type} - {file_info['year']}")
                
            except Exception as e:
                self.logger.error(f"Erro ao processar CSV para {ie_type} - {file_info['year']}: {str(e)}")
                continue
        
        if not dfs:
            self.logger.warning(f"Nenhum dado coletado para {ie_type}")
            return pd.DataFrame()
        
        result_df = pd.concat(dfs, ignore_index=True)
        self.logger.info(f"Total {ie_type}: {len(result_df)} registros")
        return result_df
    
    def collect_import_data(self) -> pd.DataFrame:
        """Coleta dados de importação do Brasil."""
        return self._process_ie_type("import")
    
    def collect_export_data(self) -> pd.DataFrame:
        """Coleta dados de exportação do Brasil."""
        return self._process_ie_type("export")
    
    def _country_specific_treatment(self, df: pd.DataFrame) -> pd.DataFrame:
        """Tratamento específico para dados do Brasil."""
        # Remover colunas que não são mais necessárias após normalização

        if 'Data' not in df.columns:
            df['Data'] = pd.to_datetime(df['CO_MES'].astype(str).str.zfill(2) + '-' + df['CO_ANO'].astype(str), format='%m-%Y')

        columns_to_drop = ['CO_MES', 'CO_ANO']
        for col in columns_to_drop:
            if col in df.columns:
                df.drop(columns=[col], inplace=True)
        
        # Garantir que coluna Data existe (já deve existir após normalização)
        if 'Data' not in df.columns:
            self.logger.warning("Coluna Data não encontrada após normalização")
        
        return df


def main():
    """Execução local do pipeline completo."""
    # Calcular start_date garantindo que o arquivo do ano anterior seja incluído
    # quando estamos nos primeiros meses do ano (ex: Jan-Mar/2026 → inclui 2025
    # para capturar Dez/2025 e evitar gaps no merge com o historical.parquet)
    today = datetime.now()
    months_lookback = 5  # meses para trás; garante cobertura do ano anterior quando necessário
    start = (today.replace(day=1) - pd.DateOffset(months=months_lookback)).to_pydatetime()

    pipeline = COMEX_BRA_NM(start_date=start, developing=True)
    
    # Executar pipeline completo
    result_df = pipeline.run()
    
    # Salvar resultado final localmente
    # timestamp = datetime.now().strftime('%Y%m%dT%H%M')
    # result_df.to_excel(f'subir_COMEX_BRA_NM_{timestamp}.xlsx', index=False)
    
    return result_df


if __name__ == "__main__":
    result = main()
    print(f"\nPipeline concluído! Total de registros: {len(result)}")