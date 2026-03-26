'''
https://api.census.gov/data/timeseries/intltrade/exports/enduse/variables.html
https://api.census.gov/data/timeseries/intltrade/imports/enduse/variables.html

DOC dos paises:
https://www.census.gov/foreign-trade/schedules/c/country.txt
'''

from datetime import datetime, timedelta
from typing import List, Dict
import sys
import os
from pathlib import Path
import pandas as pd

# Adicionar paths ao sys.path para imports
project_root = Path(__file__).parent.parent.parent
om_path = project_root / "OM"
library_path = project_root / "library"
if str(om_path) not in sys.path:
    sys.path.insert(0, str(om_path))
if str(library_path) not in sys.path:
    sys.path.insert(0, str(library_path))

from OM.costdrivers_comex_OM import ComexPipeline
from library.costdrivers import ApiAsync


class COMEX_USA(ComexPipeline):
    """Pipeline de COMEX específico para Estados Unidos (USA).
    
    Coleta dados de comércio exterior dos EUA via API U.S. Census
    e processa usando o framework ComexPipeline.
    """
    
    # URLs da API U.S. Census
    IMPORT_URL = "https://api.census.gov/data/timeseries/intltrade/imports/hs"
    EXPORT_URL = "https://api.census.gov/data/timeseries/intltrade/exports/hs"
    API_KEY = "0b09d9c5b84851e0fa3a1995cf041f0a8ac3b380"
    
    # Lista de países para filtrar (mantida do código antigo)
    USA_COMEX_PAISES = [
        'INDIA', 'SRI LANKA', 'THAILAND', 'VIETNAM', 'MALAYSIA', 'SINGAPORE', 
        'INDONESIA', 'PHILIPPINES', 'CHINA', 'AUSTRALIA', 'TRINIDAD AND TOBAGO', 
        'SWEDEN', 'NETHERLANDS', 'FRANCE', 'GERMANY', 'GHANA', 'SLOVENIA', 
        'CANADA', 'MEXICO', 'KOREA, SOUTH', 'TAIWAN', 'JAPAN', 'UNITED KINGDOM', 
        'BELGIUM', 'ITALY', 'PAKISTAN', 'PERU', 'IRELAND', 'PORTUGAL', 'HAITI', 
        'BRAZIL', 'ARGENTINA', 'TURKEY', 'BAHAMAS', 'UNITED ARAB EMIRATES', 
        'POLAND', 'SOUTH AFRICA', 'ARMENIA', 'ECUADOR', 'EL SALVADOR', 
        'BANGLADESH', 'GUYANA', 'CHILE', 'NIGERIA', 'SENEGAL', 'SPAIN', 
        'JORDAN', 'COLOMBIA', 'BOLIVIA', 'HONDURAS', 'LAOS', 'MOLDOVA', 
        'TAJIKISTAN', 'URUGUAY', 'GUATEMALA', 'CAMBODIA', 'HONG KONG', 'EGYPT', 
        'TANZANIA', 'MACEDONIA', 'KYRGYZSTAN', 'SERBIA', 'GREECE', 'ISRAEL', 
        'UGANDA', 'NEPAL', 'LITHUANIA', 'RUSSIA', 'UKRAINE', 'KAZAKHSTAN', 
        'KENYA', 'NEW ZEALAND', 'TOGO', 'VENEZUELA', 'RWANDA', 'ALBANIA', 
        'COSTA RICA', 'DOMINICAN REPUBLIC', 'ETHIOPIA', 'DJIBOUTI', 'JAMAICA', 
        'NICARAGUA', 'MOROCCO', 'CROATIA', 'ALGERIA', 'UZBEKISTAN', 'FINLAND', 
        'LATVIA', 'IRAQ', 'BELARUS', 'DENMARK', 'CAMEROON', 'TUNISIA', 'ESTONIA', 
        'CZECH REPUBLIC', 'HUNGARY', 'PARAGUAY', 'AUSTRIA', 'ROMANIA', 
        'SWITZERLAND', 'BULGARIA', 'LEBANON', 'AFGHANISTAN', 'BENIN', 'NIGER', 
        'MALTA', 'CAYMAN ISLANDS', 'ZAMBIA', 'GEORGIA', 'MADAGASCAR', 'KUWAIT', 
        'GRENADA', 'ICELAND', 'NORWAY', 'FAROE ISLANDS', 'VANUATU', 'FIJI', 
        'TONGA', 'SUDAN', 'CHAD', 'SOMALIA', 'MONACO', 'CYPRUS', 'YEMEN', 
        'SYRIA', 'SIERRA LEONE', 'LIBERIA', 'ZIMBABWE', 'PANAMA', 'BARBADOS', 
        'CABO VERDE', 'MAURITIUS', 'BURMA', 'AZERBAIJAN', 'BAHRAIN', 
        'SAUDI ARABIA', 'GREENLAND', 'MAURITANIA', 'ESWATINI', 'BELIZE', 
        'MOZAMBIQUE', 'MONGOLIA', 'SLOVAKIA', 'LUXEMBOURG', 
        'BOSNIA AND HERZEGOVINA', 'PAPUA NEW GUINEA', 'SAMOA', 'KOSOVO', 
        'SAN MARINO', 'OMAN', 'MACAU', 'ANGOLA', 'FRENCH POLYNESIA', 'MAYOTTE', 
        'GUINEA', 'SOLOMON ISLANDS', 'ARUBA', 'BERMUDA', 'MONTENEGRO', 
        'ANGUILLA', 'BOTSWANA', 'MARTINIQUE', 'IRAN', 
        'DEMOCRATIC REPUBLIC OF THE CONGO', 'MALAWI', 'NAMIBIA', 'QATAR', 
        'LIECHTENSTEIN', 'MARSHALL ISLANDS', 'GABON', 'NEW CALEDONIA', 'LIBYA', 
        'EQUATORIAL GUINEA', 'CURACAO', 'TURKMENISTAN', 'BRUNEI', 'SEYCHELLES', 
        'COMOROS', 'DOMINICA', 'GUADELOUPE', 'COCOS (KEELING) ISLANDS', 'MALI', 
        'SURINAME', 'BRITISH VIRGIN ISLANDS', 'BURKINA FASO', 'MICRONESIA', 
        'CUBA', 'TIMOR-LESTE', 'NAURU', 'CONGO', 'GAMBIA', 
        'CENTRAL AFRICAN REPUBLIC', 'TURKS AND CAICOS ISLANDS', 
        'SAO TOME AND PRINCIPE', 'ANTIGUA AND BARBUDA', 'ERITREA', 
        'FRENCH GUIANA', 'CHRISTMAS ISLAND', 'MALDIVES', 'GIBRALTAR', 
        'MONTSERRAT', 'ANDORRA', 'NIUE', 'LESOTHO', 'KIRIBATI', 'COOK ISLANDS', 
        'BURUNDI', 'BHUTAN', 'GUINEA-BISSAU'
    ]
    
    def __init__(self, start_date=None, use_azure=True):
        """
        Inicializa pipeline COMEX para Estados Unidos.
        
        Args:
            start_date: Data inicial para coleta (padrão: 3 meses atrás, excluindo últimos 2 meses)
            use_azure: Se deve usar Azure Storage
        """
        super().__init__(
            iso_code='USA',
            start_date=start_date,
            data_contract_path='data-contract.yaml',
            ids_table_path='IDS_comex.xlsx',
            use_azure=use_azure
        )
        
        # Carregar arquivo NCM_IE_USITC.xlsx
        self.ncm_file_path = "NCM_IE_USITC.xlsx"
        self.ncm_file_path = os.getcwd() + "\OM\COMEX_USA\\" + self.ncm_file_path    
        self.df_ncm = self._load_ncm_file()
        
        # Construir range de datas (últimos 3 meses, excluindo últimos 2 meses)
        if start_date is None:
            # Padrão: 3 meses atrás, excluindo últimos 2 meses
            self.start_period = (datetime.now() - timedelta(days=365*1)).replace(day=1)
        else:
            self.start_period = start_date
        
        self.date_range = self._build_date_range()
    
    def _get_country_name(self) -> str:
        """Retorna nome do país para filtro na tabela de IDs."""
        return 'Estados Unidos'
    
    def _load_ncm_file(self) -> pd.DataFrame:
        """
        Carrega arquivo Excel com NCMs e tipos de comércio.
        
        Returns:
            DataFrame com colunas IDIndicePrincipal, IE, NCM
        """
        try:
            df = pd.read_excel(self.ncm_file_path)
            self.logger.info(f"Arquivo NCM carregado: {len(df)} registros")
            return df
        except FileNotFoundError:
            self.logger.error(f"Arquivo {self.ncm_file_path} não encontrado")
            return pd.DataFrame()
        except Exception as e:
            self.logger.error(f"Erro ao carregar arquivo NCM: {str(e)}")
            return pd.DataFrame()
    
    def _build_date_range(self) -> List[str]:
        """
        Constrói range de datas para coleta (formato YYYY-MM).
        Exclui os últimos 2 meses.
        
        Returns:
            Lista de strings no formato YYYY-MM
        """
        dates = pd.date_range(
            start=self.start_period,
            end=pd.Timestamp.today(),
            freq="MS"
        )
        # Excluir últimos 2 meses
        date_range = dates.strftime("%Y-%m").tolist()[:-2]
        self.logger.info(f"Range de datas construído: {len(date_range)} meses (de {date_range[0] if date_range else 'N/A'} até {date_range[-1] if date_range else 'N/A'})")
        return date_range
    
    def _mount_requests_for_month(self, month: str, ie_type: str) -> List[Dict]:
        """
        Monta lista de requisições para um mês específico.
        
        Args:
            month: Mês no formato YYYY-MM
            ie_type: 'import' ou 'export'
            
        Returns:
            Lista de dicionários com requisições para ApiAsync
        """
        requests_list = []
        
        # Filtrar NCMs por tipo (IE: 1 = Import, 0 = Export)
        ie_value = 1 if ie_type == 'import' else 0
        df_filtered = self.df_ncm[self.df_ncm['IE'] == ie_value].copy()
        
        if df_filtered.empty:
            self.logger.warning(f"Nenhum NCM encontrado para {ie_type}")
            return requests_list
        
        for _, row in df_filtered.iterrows():
            ncm = str(row['NCM'])
            
            if ie_type == 'import':
                params = {
                    "get": "I_COMMODITY,CTY_CODE,CTY_NAME,GEN_VAL_MO,GEN_CIF_MO,GEN_QY1_MO",
                    "time": month,
                    "I_COMMODITY": ncm,
                    "COMM_LVL": "HS10",
                    "key": self.API_KEY,
                }
                requests_list.append({
                    "url": self.IMPORT_URL,
                    "method": "get",
                    "params": params,
                    "NCM": ncm
                })
            else:  # export
                params = {
                    "get": "E_COMMODITY,CTY_CODE,CTY_NAME,ALL_VAL_MO,QTY_1_MO",
                    "time": month,
                    "E_COMMODITY": ncm,
                    "COMM_LVL": "HS10",
                    "key": self.API_KEY,
                }
                requests_list.append({
                    "url": self.EXPORT_URL,
                    "method": "get",
                    "params": params,
                    "NCM": ncm
                })
        
        return requests_list
    
    def _fetch_month(self, month: str) -> Dict[str, List[pd.DataFrame]]:
        """
        Coleta dados de um mês específico (import e export).
        
        Args:
            month: Mês no formato YYYY-MM
            
        Returns:
            Dicionário com listas de DataFrames para import e export
        """
        self.logger.info(f"Coletando dados de {month}...")
        
        # Montar requisições
        reqs_import = self._mount_requests_for_month(month, 'import')
        reqs_export = self._mount_requests_for_month(month, 'export')
        
        # Executar requisições assíncronas
        dados_import = []
        dados_export = []
        
        if reqs_import:
            try:
                dados_import = ApiAsync(True, lista=reqs_import).run()
            except Exception as e:
                self.logger.error(f"Erro ao coletar dados de importação para {month}: {str(e)}")
        
        if reqs_export:
            try:
                dados_export = ApiAsync(True, lista=reqs_export).run()
            except Exception as e:
                self.logger.error(f"Erro ao coletar dados de exportação para {month}: {str(e)}")
        
        # Processar respostas em DataFrames
        dfs_import = [
            pd.DataFrame(c) for c in dados_import if isinstance(c, list)
        ]
        
        dfs_export = [
            pd.DataFrame(c) for c in dados_export if isinstance(c, list)
        ]
        
        self.logger.info(f"Dados de {month} coletados: {len(dfs_import)} DataFrames import, {len(dfs_export)} DataFrames export")
        
        return {"import": dfs_import, "export": dfs_export}
    
    def collect_import_data(self) -> pd.DataFrame:
        """
        Coleta dados de importação dos Estados Unidos.
        
        Returns:
            DataFrame com dados de importação
        """
        list_df_import = []
        
        for month in self.date_range:
            self.logger.info(f"Processando mês: {month}")
            result = self._fetch_month(month)
            list_df_import.extend(result["import"])
        
        if not list_df_import:
            self.logger.warning("Nenhum dado de importação coletado")
            return pd.DataFrame()
        
        df_import = pd.concat(list_df_import, ignore_index=True)
        self.logger.info(f"Total importação coletado: {len(df_import)} registros")
        return df_import
    
    def collect_export_data(self) -> pd.DataFrame:
        """
        Coleta dados de exportação dos Estados Unidos.
        
        Returns:
            DataFrame com dados de exportação
        """
        list_df_export = []
        
        for month in self.date_range:
            self.logger.info(f"Processando mês: {month}")
            result = self._fetch_month(month)
            list_df_export.extend(result["export"])
        
        if not list_df_export:
            self.logger.warning("Nenhum dado de exportação coletado")
            return pd.DataFrame()
        
        df_export = pd.concat(list_df_export, ignore_index=True)
        self.logger.info(f"Total exportação coletado: {len(df_export)} registros")
        return df_export
    
    def _country_specific_treatment(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Tratamento específico para dados dos Estados Unidos.
        
        - Filtra por lista de países
        - Para importações: calcula frete + seguro a partir de valor_cif - valor
        - Para exportações: adiciona frete e seguro = 0
        
        Args:
            df: DataFrame após normalização de colunas
            
        Returns:
            DataFrame tratado
        """
        if df.empty:
            return df
        
        # Filtrar por lista de países (pais_name vem da normalização de CTY_NAME)
        if 'pais_name' in df.columns:
            df = df[df['pais_name'].isin(self.USA_COMEX_PAISES)].copy()
            self.logger.info(f"Dados filtrados por países: {len(df)} registros")
        
        if 'peso' in df.columns and 'valor' in df.columns:
            # Converter para float
            df['peso'] = df['peso'].astype(float)
            df['valor'] = df['valor'].astype(float)
        
        # Adicionar colunas frete e seguro com valor 0 (assumindo que não existem na API)
        if 'frete' not in df.columns:
            df['frete'] = 0.0
        if 'seguro' not in df.columns:
            df['seguro'] = 0.0
        
        # Garantir que coluna Data está em formato datetime
        if 'Data' in df.columns:
            if not pd.api.types.is_datetime64_any_dtype(df['Data']):
                df['Data'] = pd.to_datetime(df['Data'], format='%Y-%m', errors='coerce')
        
        return df


def main():
    """Execução local do pipeline completo."""
    # Criar instância do pipeline
    pipeline = COMEX_USA(start_date=None)
    
    # Executar pipeline completo
    result_df = pipeline.run()
    
    # Salvar resultado final localmente
    # timestamp = datetime.now().strftime('%Y%m%dT%H%M')
    # result_df.to_excel(f'subir_COMEX_USA_{timestamp}.xlsx', index=False)
    
    return result_df


if __name__ == "__main__":
    result = main()
    print(f"\nPipeline concluído! Total de registros: {len(result)}")

