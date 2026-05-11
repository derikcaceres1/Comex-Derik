"""
Processador COMEX para Estados Unidos.
Usa Polars para processamento de dados de import/export.
"""
import os
import pandas as pd
import polars as pl
from NM.globinho.comex_globe_pipeline import BaseComexProcessor


def normalize_country(x: str) -> str:
    """
    Normaliza nome de país para matching.
    
    Args:
        x: Nome do país
        
    Returns:
        str: Nome normalizado
    """
    if "," in x:
        return " ".join(
            reversed(
                x.lower()
                .strip()
                .replace(",", "")
                .split()
            )
        )
    else:
        return (
            x.lower()
            .strip()
            .replace("-", " ")
            .replace(".", "")
            .replace("'", "")
        )


class COMEX_USA_GLOBE(BaseComexProcessor):
    """Processador COMEX para dados dos Estados Unidos."""
    
    def load_raw_data(self) -> pl.LazyFrame:
        """
        Carrega dados brutos dos EUA (import e export) usando Polars.
        
        Returns:
            pl.LazyFrame: Dados agregados
        """
        file_import_str = self.config['paths']['usa']['import']
        file_export_str = self.config['paths']['usa']['export']
        file_import = self._find_file(file_import_str, file_type="de importação")
        file_export = self._find_file(file_export_str, file_type="de exportação")
        base_path = self.config['paths']['usa']['aggregated']
        
        os.makedirs(base_path, exist_ok=True)
        
        # Export
        df_e = (
            pl.scan_parquet(str(file_export))
            .select([
                'E_COMMODITY',
                'CTY_NAME',
                'ALL_VAL_MO',
                'QTY_1_MO',
                'time'
            ])
            .rename({
                'E_COMMODITY': 'ncm',
                'CTY_NAME': 'pais',
                'QTY_1_MO': 'peso',
                'ALL_VAL_MO': 'fob',
            })
            .with_columns([
                pl.col('ncm')
                .cast(pl.Utf8)
                .str.strip_chars()
                .str.zfill(10)
                .cast(pl.Categorical),
                pl.col('pais').cast(pl.Utf8),
                pl.col('time').cast(pl.Utf8),
                pl.col('peso').cast(pl.Float64, strict=False),
                pl.col('fob').cast(pl.Float64, strict=False),
                pl.lit(0).cast(pl.Int8).alias('ImportExport'),  # export
            ])
            .filter(
                pl.col('fob') > 0,
                pl.col('peso') > 0,
                pl.col('ncm').is_not_null(),
                pl.col('pais').is_not_null(),
            )
        )
        
        # Import
        df_i = (
            pl.scan_parquet(str(file_import))
            .select([
                'I_COMMODITY',
                'CTY_NAME',
                'GEN_CIF_MO',
                'GEN_QY1_MO',
                'time'
            ])
            .rename({
                'I_COMMODITY': 'ncm',
                'CTY_NAME': 'pais',
                'GEN_QY1_MO': 'peso',
                'GEN_CIF_MO': 'fob',
            })
            .with_columns([
                pl.col('ncm')
                .cast(pl.Utf8)
                .str.strip_chars()
                .str.zfill(10)
                .cast(pl.Categorical),
                pl.col('pais').cast(pl.Utf8),
                pl.col('time').cast(pl.Utf8),
                pl.col('peso').cast(pl.Float64, strict=False),
                pl.col('fob').cast(pl.Float64, strict=False),
                pl.lit(1).cast(pl.Int8).alias('ImportExport'),  # import
            ])
            .filter(
                pl.col('fob') > 0,
                pl.col('peso') > 0,
                pl.col('ncm').is_not_null(),
                pl.col('pais').is_not_null(),
            )
        )
        
        # Concatenar e agrupar
        df = (
            pl.concat([df_e, df_i])
            .group_by(['time', 'ncm', 'ImportExport', 'pais'])
            .agg([
                pl.sum('peso').alias('peso'),
                pl.sum('fob').alias('fob'),
            ])
        )
        
        # Salvar agregado (opcional, para cache)
        df.sink_parquet(
            pl.PartitionByKey(
                base_path,
                by="time"
            ),
            compression="zstd",
            mkdir=True
        )
        
        # Retornar LazyFrame
        aggregated_path = self._find_file(base_path, file_type="aggregated")
        aggregated_lf = pl.scan_parquet(str(aggregated_path))
        return aggregated_lf
    
    def transform_data(self, lf: pl.LazyFrame) -> pl.LazyFrame:
        """
        Transforma dados dos EUA: converte time para dataNCM.
        
        Args:
            lf: LazyFrame com dados agregados
            
        Returns:
            pl.LazyFrame: Dados transformados
        """
        # Converter time para dataNCM
        lf = lf.with_columns([
            pl.col('time')
            .str.strptime(pl.Date, "%Y-%m")
            .alias("dataNCM")
        ])
        
        # Remover time
        lf = lf.drop(['time'])
        
        # Renomear pais para idPais
        lf = lf.rename({'pais': 'idPais'})
        
        return lf
    
    def normalize_countries(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Normaliza IDs de países dos EUA usando normalização customizada de nomes.
        
        Args:
            df: DataFrame pandas com dados processados
            
        Returns:
            pd.DataFrame: DataFrame com países normalizados
        """
        # Carregar tabela de países da plataforma
        pais_table_path = self._find_file(self.config['paths']['pais_table'], file_type="tabela de países")
        paisID = pd.read_excel(pais_table_path)
        
        # Normalizar nomes de países
        paisID['pais_normalized'] = paisID['Pais_2'].apply(normalize_country)
        df['pais_normalized'] = df['idPais'].apply(normalize_country)
        
        # Merge usando nome normalizado
        df = df.merge(paisID, on='pais_normalized', how='inner')
        df['idPais'] = df['IDNCMPais']
        df = df.drop(columns=['IDNCMPais', 'pais_normalized'])
        
        # Selecionar apenas colunas necessárias
        df = df[['dataNCM', 'idIndicador', 'idPais', 'peso', 'fob']]
        
        return df


def main(config_path: str = 'config_comex.json'):
    """
    Função principal que executa o pipeline completo para Estados Unidos.
    
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
    print("COMEX ESTADOS UNIDOS - Pipeline de Processamento")
    print("=" * 60)
    
    processor = COMEX_USA_GLOBE(config_path)
    results = processor.process()
    
    print(f"\nProcessamento concluído!")
    print(f"Total de batches processados: {len(results)}")
    print(f"Batches com sucesso: {sum(1 for r in results if r.get('status') == 200)}")
    print(f"Batches com erro: {sum(1 for r in results if r.get('status') != 200)}")
    
    return results


if __name__ == "__main__":
    main()

