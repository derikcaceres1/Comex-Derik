"""
Processador COMEX para Brasil.
Migrado de pandas para Polars conforme arquitetura unificada.
"""
import pandas as pd
import polars as pl
from NM.globinho.comex_globe_pipeline import BaseComexProcessor


class COMEX_BRA_GLOBE(BaseComexProcessor):
    """Processador COMEX para dados do Brasil."""
    
    def load_raw_data(self) -> pl.LazyFrame:
        """
        Carrega dados brutos do Brasil usando Polars.
        
        Returns:
            pl.LazyFrame: Dados brutos carregados
        """
        database_file_str = self.config['paths']['bra']['database']
        database_file = self._find_file(database_file_str, file_type="database")
        lf = pl.scan_parquet(str(database_file))
        return lf
    
    def transform_data(self, lf: pl.LazyFrame) -> pl.LazyFrame:
        """
        Transforma dados do Brasil: renomeia colunas, cria data, filtra por data máxima.
        
        Args:
            lf: LazyFrame com dados brutos
            
        Returns:
            pl.LazyFrame: Dados transformados
        """
        # Renomear colunas
        lf = lf.rename({
            'CO_NCM': 'ncm',
            'CO_PAIS': 'pais',
            'KG_LIQUIDO': 'peso',
            'VL_FOB': 'fob',
            'VL_FRETE': 'frete',
            'VL_SEGURO': 'seguro',
        })
        
        # Criar coluna data a partir de CO_ANO e CO_MES
        lf = lf.with_columns([
            (
                pl.col('CO_ANO').cast(pl.Utf8) + 
                pl.lit('-') + 
                pl.col('CO_MES').cast(pl.Utf8).str.zfill(2)
            ).str.strptime(pl.Date, "%Y-%m").alias('data')
        ])
        
        # Remover colunas CO_ANO e CO_MES
        lf = lf.drop(['CO_ANO', 'CO_MES'])
        
        # Encontrar data máxima e filtrar
        # Primeiro coletamos para obter a data máxima
        df_temp = lf.select(['data']).collect()
        max_date = df_temp['data'].max()
        
        # Filtrar por data máxima
        lf = lf.filter(pl.col('data') == max_date)
        
        # Renomear data para dataNCM (será usado depois)
        lf = lf.rename({'data': 'dataNCM'})
        
        # Renomear pais para idPais
        lf = lf.rename({'pais': 'idPais'})
        
        # ImportExport deve existir nos dados originais do Brasil
        # Se não existir, precisamos adicionar (assumindo 1 para import, 0 para export)
        # Por enquanto, assumimos que existe nos dados
        
        return lf
    
    def normalize_countries(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Normaliza IDs de países do Brasil usando tabela de mapeamento.
        
        Args:
            df: DataFrame pandas com dados processados
            
        Returns:
            pd.DataFrame: DataFrame com países normalizados
        """
        # Carregar tabela de países da plataforma
        pais_table_path = self._find_file(self.config['paths']['pais_table'], file_type="tabela de países")
        paisID = pd.read_excel(pais_table_path)
        
        # Carregar tabela de países da fonte
        pais_source_path = self._find_file(self.config['paths']['bra']['pais_source'], file_type="fonte de países")
        paisID_source = pd.read_csv(
            pais_source_path,
            encoding="ISO-8859-1",
            sep=';'
        )
        
        # Fazer merge para normalizar
        columns_name = 'CO_PAIS'
        paisID_normalized = paisID_source.merge(
            paisID,
            left_on='NO_PAIS',
            right_on='Pais_1',
            how='inner'
        )
        paisID_normalized = paisID_normalized[[columns_name, 'IDNCMPais']]
        paisID_normalized.rename(columns={columns_name: 'idPais'}, inplace=True)
        
        # Merge com dados principais
        df = df.merge(paisID_normalized, on='idPais', how='inner')
        df['idPais'] = df['IDNCMPais']
        df = df.drop(columns=['IDNCMPais'])
        
        return df


def main(config_path: str = 'config_comex.json'):
    """
    Função principal que executa o pipeline completo para Brasil.
    
    Fluxo:
    1. Coleta do histórico (load_raw_data)
    2. Transformação e agrupamento (transform_data)
    3. Merge para encontrar o ID do indicador (merge_with_indicators)
    4. Merge para encontrar o paisID (normalize_countries)
    5. Upload para API (upload_to_api)
    
    Args:
        config_path: Caminho para o arquivo de configuração
        
    Returns:
        List[Dict]: Resultados do upload
    """
    print("=" * 60)
    print("COMEX BRASIL - Pipeline de Processamento")
    print("=" * 60)
    
    processor = COMEX_BRA_GLOBE(config_path)
    results = processor.process()
    
    print(f"\nProcessamento concluído!")
    print(f"Total de batches processados: {len(results)}")
    print(f"Batches com sucesso: {sum(1 for r in results if r.get('status') == 200)}")
    print(f"Batches com erro: {sum(1 for r in results if r.get('status') != 200)}")
    
    return results


if __name__ == "__main__":
    main()

