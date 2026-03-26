from datetime import datetime
import sys
import os
from pathlib import Path
import pandas as pd

# Adicionar path do diretório OM ao sys.path para imports
project_root = Path(__file__).parent.parent.parent
om_path = project_root / "OM"
if str(om_path) not in sys.path:
    sys.path.insert(0, str(om_path))

from OM.costdrivers_comex_OM import ComexPipeline


class COMEX_DEU(ComexPipeline):
    """Pipeline de COMEX específico para Alemanha (DEU).
    
    Processa dados da Alemanha a partir dos dados já coletados do EUR no ADLS,
    filtrando por pais_origem == 'DE'.
    """
    
    def __init__(self, start_date=None, use_azure=True):
        super().__init__(
            iso_code='DEU',
            start_date=start_date,
            data_contract_path='include/data-contract.yaml',
            ids_table_path='library/IDS_comex.xlsx',
            use_azure=use_azure
        )

        # Usar OM/dados/EUR/raw quando não estiver usando Azure
        if not self.use_azure:
            self.raw_path = 'OM/dados/EUR/raw'
        else:
            self.raw_path = 'staging/comex/EUR/raw'
    
    def _get_country_name(self) -> str:
        """Retorna nome do país para filtro na tabela de IDs."""
        return 'Alemanha'
    
    def _filter_country(self, df_eur: pd.DataFrame, data_type: str = "dados") -> pd.DataFrame:
        """
        Filtra dados do EUR para manter apenas registros da Alemanha (DE).
        
        Args:
            df_eur: DataFrame com dados do EUR
            data_type: Tipo de dados para mensagens de log (ex: "importação", "exportação")
            
        Returns:
            DataFrame filtrado com dados da Alemanha
        """
        if df_eur.empty:
            self.logger.warning(f"Nenhum dado de {data_type} encontrado no EUR")
            return pd.DataFrame()
        
        self.logger.info(f"Dados EUR carregados: {len(df_eur)} registros")
        
        # Identificar coluna para filtro (pais_origem ou REPORTER)
        col = None
        if 'pais_origem' in df_eur.columns:
            col = 'pais_origem'
        elif 'REPORTER' in df_eur.columns:
            col = 'REPORTER'
        else:
            self.logger.error("Nenhuma coluna de país encontrada (pais_origem ou REPORTER)")
            return pd.DataFrame()
        
        # Filtrar por país == 'DE'
        df_deu = df_eur[df_eur[col] == 'DE'].copy()
        
        # Log do resultado do filtro
        if df_deu.empty:
            self.logger.warning(f"Nenhum registro da Alemanha encontrado após filtro por {col} == 'DE'")
        else:
            self.logger.info(f"Dados DEU filtrados: {len(df_deu)} registros (de {len(df_eur)} total) - {col} == 'DE'")
        
        return df_deu
    
    def collect_import_data(self) -> pd.DataFrame:
        """
        Coleta dados de importação da Alemanha.
        Carrega dados do ADLS de EUR/raw e filtra por pais_origem == 'DE'.
        
        Returns:
            DataFrame com dados de importação da Alemanha
        """
        self.logger.info("Carregando dados de importação do EUR do ADLS...")
        
        # Carregar dados do EUR do ADLS
        # Usar OM/dados/EUR/raw quando não estiver usando Azure
        if not self.use_azure:
            eur_raw_path = "OM/dados/EUR/raw"
        else:
            eur_raw_path = "staging/comex/EUR/raw"
        df_eur = self._load_from_storage(eur_raw_path, "import_raw.parquet")
        df_deu = self._filter_country(df_eur, data_type="importação")
        return df_deu
    
    def collect_export_data(self) -> pd.DataFrame:
        """
        Coleta dados de exportação da Alemanha.
        Carrega dados do ADLS de EUR/raw e filtra por pais_origem == 'DE'.
        
        Returns:
            DataFrame com dados de exportação da Alemanha
        """
        self.logger.info("Carregando dados de exportação do EUR do ADLS...")
        
        # Carregar dados do EUR do ADLS
        # Usar OM/dados/EUR/raw quando não estiver usando Azure
        if not self.use_azure:
            eur_raw_path = "OM/dados/EUR/raw"
        else:
            eur_raw_path = "staging/comex/EUR/raw"
        df_eur = self._load_from_storage(eur_raw_path, "export_raw.parquet")
        df_deu = self._filter_country(df_eur, data_type="exportação")
        return df_deu
    
    def _country_specific_treatment(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Tratamento específico para dados da Alemanha.
        Segue a lógica do código antigo: filtra TOTAL, mapeia TRADE_TYPE, remove colunas desnecessárias.
        
        Args:
            df: DataFrame após normalização de colunas
            
        Returns:
            DataFrame tratado
        """
        # Filtrar linhas onde NCM == "TOTAL" (se ainda não filtrado)
        if 'ncm' in df.columns:
            df = df[df['ncm'] != "TOTAL"].copy()
        
        # Mapear TRADE_TYPE para ImportExport se TRADE_TYPE existir e ImportExport não existir
        if 'TRADE_TYPE' in df.columns and 'ImportExport' not in df.columns:
            IE_map = {
                'I': 1,
                'E': 0
            }
            df['ImportExport'] = df['TRADE_TYPE'].map(IE_map)
            # Remover coluna TRADE_TYPE após mapeamento
            df.drop(columns=['TRADE_TYPE'], inplace=True)
        
        # Remover coluna Pais se existir (não é necessária após normalização)
        if 'pais' in df.columns:
            df.drop(columns=['pais'], inplace=True)
        
        # ajuste das datas
        df['Data'] = pd.to_datetime(df['Data'], format='%Y%m')
        
        return df


def main():
    """Execução local do pipeline completo."""
    # Criar instância do pipeline
    pipeline = COMEX_DEU(start_date=None, use_azure=False)
    
    # Executar pipeline completo
    result_df = pipeline.run()
    
    # Salvar resultado final localmente
    # timestamp = datetime.now().strftime('%Y%m%dT%H%M')
    # result_df.to_excel(f'subir_COMEX_DEU_{timestamp}.xlsx', index=False)
    
    return result_df


if __name__ == "__main__":
    result = main()
    print(f"\nPipeline concluído! Total de registros: {len(result)}")

