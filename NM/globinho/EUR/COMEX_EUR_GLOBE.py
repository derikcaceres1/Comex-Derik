"""
Processador COMEX para União Europeia.
Usa Polars para processamento por períodos.
"""
import os
import pandas as pd
import polars as pl
from io import StringIO
from NM.globinho.comex_globe_pipeline import BaseComexProcessor


class COMEX_EUR_GLOBE(BaseComexProcessor):
    """Processador COMEX para dados da União Europeia."""
    
    def load_raw_data(self) -> pl.LazyFrame:
        """
        Carrega dados brutos da UE processando por períodos.
        
        Returns:
            pl.LazyFrame: Dados agregados por período
        """
        database_file_str = self.config['paths']['eur']['database']
        database_file = self._find_file(database_file_str, file_type="database")
        base_path = self.config['paths']['eur']['aggregated']
        
        # Criar diretório se não existir
        os.makedirs(base_path, exist_ok=True)
        
        # Obter períodos únicos
        periods = (
            pl.scan_parquet(str(database_file))
            .select('PERIOD')
            .unique()
            .collect()
            .to_series()
            .to_list()
        )
        
        print(f"{len(periods)} períodos encontrados")
        
        # Processar cada período
        for p in periods:
            print(f"Processando PERIOD={p}")
            df_p = (
                pl.scan_parquet(str(database_file))
                .select([
                    'PRODUCT_NC',
                    'PARTNER',
                    'QUANTITY_KG',
                    'VALUE_EUR',
                    'TRADE_TYPE',
                    'PERIOD',
                ])
                .filter(pl.col('PERIOD') == p)
                .rename({
                    'PRODUCT_NC': 'ncm',
                    'PARTNER': 'pais',
                    'QUANTITY_KG': 'peso',
                    'VALUE_EUR': 'fob',
                    'TRADE_TYPE': 'ImportExport',
                })
                .filter(pl.col('ncm').is_not_null())
                .with_columns([
                    pl.col('ncm').cast(pl.Int32),
                    pl.col('ImportExport').cast(pl.Utf8),
                    pl.col('pais').cast(pl.Utf8),
                    pl.col('PERIOD').cast(pl.Utf8),
                ])
                .group_by(['PERIOD', 'ncm', 'ImportExport', 'pais'])
                .agg([
                    pl.sum('peso').alias('peso'),
                    pl.sum('fob').alias('fob'),
                ])
            )
            # Escrever período
            df_p.sink_parquet(
                os.path.join(base_path, f"PERIOD={p}"),
                compression="zstd",
                mkdir=True
            )
        
        # Retornar LazyFrame dos dados agregados
        aggregated_path = self._find_file(base_path, file_type="aggregated")
        aggregated_lf = pl.scan_parquet(str(aggregated_path))
        return aggregated_lf
    
    def transform_data(self, lf: pl.LazyFrame) -> pl.LazyFrame:
        """
        Transforma dados da UE: converte PERIOD para dataNCM.
        
        Args:
            lf: LazyFrame com dados agregados
            
        Returns:
            pl.LazyFrame: Dados transformados
        """
        # Converter PERIOD para dataNCM
        lf = lf.with_columns([
            pl.col('PERIOD')
            .str.strptime(pl.Date, "%Y%m")
            .alias("dataNCM")
        ])
        
        # Remover PERIOD
        lf = lf.drop(['PERIOD'])
        
        # Renomear pais para idPais
        lf = lf.rename({'pais': 'idPais'})
        
        return lf
    
    def normalize_countries(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Normaliza IDs de países da UE usando PARTNERS_ISO.txt.
        
        Args:
            df: DataFrame pandas com dados processados
            
        Returns:
            pd.DataFrame: DataFrame com países normalizados
        """
        # Carregar tabela de países da plataforma
        pais_table_path = self._find_file(self.config['paths']['pais_table'], file_type="tabela de países")
        paisID = pd.read_excel(pais_table_path)
        
        # Carregar tabela de países da fonte (arquivo texto)
        txt_path = self._find_file(self.config['paths']['eur']['pais_source'], file_type="fonte de países")
        with open(txt_path, 'r', encoding='utf-8') as file:
            txt = file.read()
        
        # Ler como dataframe
        paisID_source = pd.read_csv(
            StringIO(txt),
            sep="\t",
            header=None,
            names=["partner", "unused_1", "unused_2", "country_name"]
        )
        
        # Dropar colunas unused
        paisID_source.drop(columns=["unused_1", "unused_2"], inplace=True)
        
        # Fazer merge para normalizar
        columns_name = 'partner'
        paisID_normalized = paisID_source.merge(
            paisID,
            left_on='country_name',
            right_on='Pais_2',
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
    Função principal que executa o pipeline completo para União Europeia.
    
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
    print("COMEX UNIÃO EUROPEIA - Pipeline de Processamento")
    print("=" * 60)
    
    processor = COMEX_EUR_GLOBE(config_path)
    results = processor.process()
    
    print(f"\nProcessamento concluído!")
    print(f"Total de batches processados: {len(results)}")
    print(f"Batches com sucesso: {sum(1 for r in results if r.get('status') == 200)}")
    print(f"Batches com erro: {sum(1 for r in results if r.get('status') != 200)}")
    
    return results


if __name__ == "__main__":
    main()

