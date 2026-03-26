from datetime import datetime
import pandas as pd
import numpy as np
import sys
import os
from pathlib import Path

# Adicionar path do diretório serie_temporal ao sys.path para imports
project_root = Path(__file__).parent.parent.parent.parent
serie_temporal_path = project_root / "NM" / "serie_temporal"
if str(serie_temporal_path) not in sys.path:
    sys.path.insert(0, str(serie_temporal_path))

from costdrivers_comex_NM import ComexPipelineNM


class COMEX_NLD_NM(ComexPipelineNM):
    """Pipeline de COMEX específico para Holanda (NLD) usando Nova Metodologia.
    
    Processa dados da Holanda a partir dos dados já coletados do EUR no ADLS,
    filtrando por pais_origem == 'NL' ou REPORTER == 'NL'.
    Usa histórico compartilhado do EUR através de iso_database='EUR'.
    """
    
    def __init__(self, start_date=None, use_azure=True, developing=False):
        super().__init__(
            iso_code='NLD',
            start_date=start_date,
            data_contract_path='data-contract.yaml',
            ids_table_path='IDS_comex.xlsx',
            use_azure=use_azure,
            developing=developing,
            iso_database='EUR'  # Usar histórico compartilhado do EUR
        )
    
    def _get_country_name(self) -> str:
        """Retorna nome do país para filtro na tabela de IDs."""
        return 'Holanda'
    
    def _country_specific_treatment(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Tratamento específico para dados da Holanda.
        Segue a lógica do código antigo: filtra TOTAL, mapeia TRADE_TYPE, remove colunas desnecessárias.
        
        Args:
            df: DataFrame após normalização de colunas
            
        Returns:
            DataFrame tratado
        """
        # Filtrar linhas onde NCM == "TOTAL" (se ainda não filtrado)
        if 'PRODUCT_NC' in df.columns:
            df = df[df['PRODUCT_NC'] != "TOTAL"].copy()
            df = df[~df['PRODUCT_NC'].isnull()].copy()
        
        # Tentar criar a partir de PERIOD se existir
        if 'PERIOD' in df.columns:
            df['Data'] = pd.to_datetime(df['PERIOD'].astype(str), format='%Y%m', errors='coerce')
            df.drop(columns=['PERIOD'], inplace=True)

        # Garantir que coluna Data existe e está no formato correto
        if 'Data' not in df.columns:
            self.logger.warning("Coluna Data não encontrada após normalização")
            return
        
        # Tratar valores inválidos em PRODUCT_NC antes da conversão
        if 'PRODUCT_NC' in df.columns:
            df['PRODUCT_NC'] = pd.to_numeric(df['PRODUCT_NC'], errors='coerce').astype('Int64')
            df.dropna(subset=['PRODUCT_NC'], inplace=True)
        
        df = df.astype({
            'PRODUCT_NC': 'int32',
        })

        df['frete'] = np.nan
        df['seguro'] = np.nan

        df.rename(columns={'TRADE_TYPE':'ImportExport'}, inplace=True)
        df.drop(columns=['REPORTER'], inplace=True)

        return df


def main():
    """Execução local do pipeline completo."""
    # Criar instância do pipeline
    pipeline = COMEX_NLD_NM(start_date=None, developing=True)
    
    # Executar pipeline completo
    result_df = pipeline.run()
    
    # Salvar resultado final localmente
    # timestamp = datetime.now().strftime('%Y%m%dT%H%M')
    # result_df.to_excel(f'subir_COMEX_NLD_NM_{timestamp}.xlsx', index=False)
    
    return result_df


if __name__ == "__main__":
    result = main()
    print(f"\nPipeline concluído! Total de registros: {len(result)}")