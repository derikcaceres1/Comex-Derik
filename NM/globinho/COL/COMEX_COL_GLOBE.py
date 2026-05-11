"""
Processador COMEX para Colômbia.
Usa Polars para processamento de dados de import/export.
"""
import os
import pandas as pd
import polars as pl
from NM.globinho.comex_globe_pipeline import BaseComexProcessor


class COMEX_COL_GLOBE(BaseComexProcessor):
    """Processador COMEX para dados da Colômbia."""
    
    def load_raw_data(self) -> pl.LazyFrame:
        """
        Carrega dados brutos da Colômbia (import e export) usando Polars.
        
        Returns:
            pl.LazyFrame: Dados agregados
        """
        file_import_str = self.config['paths']['col']['import']
        file_export_str = self.config['paths']['col']['export']
        file_import = self._find_file(file_import_str, file_type="de importação")
        file_export = self._find_file(file_export_str, file_type="de exportação")
        base_path = self.config['paths']['col']['aggregated']
        
        os.makedirs(base_path, exist_ok=True)
        
        # Export
        df_e = (
            pl.scan_parquet(str(file_export))
            .select([
                'POSAR',
                'PAIS',
                'FOBDOL',
                'PBK',
                'Data'
            ])
            .rename({
                'POSAR': 'ncm',
                'PAIS': 'pais',
                'PBK': 'peso',
                'FOBDOL': 'fob',
            })
            .filter(
                pl.col('Data').is_not_null(),
                pl.col('Data').str.contains(r"^\d{4}$")
            )
            .with_columns([
                pl.col('ncm')
                .cast(pl.Utf8)
                .str.strip_chars()
                .str.zfill(10)
                .cast(pl.Categorical),
                pl.col('pais').cast(pl.Categorical),
                pl.col('Data').cast(pl.Utf8),
                pl.col('peso')
                .cast(pl.Utf8)
                .str.strip_chars()
                .str.replace_all(r"\.", "")
                .str.replace_all(",", ".")
                .cast(pl.Float64, strict=False),
                pl.col('fob')
                .cast(pl.Utf8)
                .str.strip_chars()
                .str.replace_all(r"\.", "")
                .str.replace_all(",", ".")
                .cast(pl.Float64, strict=False),
                pl.lit(0).cast(pl.Int8).alias('ImportExport'),  # export
            ])
            .filter(
                pl.col('fob').is_not_null(),
                pl.col('fob') > 0,
                pl.col('peso').is_not_null(),
                pl.col('peso') > 0,
                pl.col('ncm').is_not_null(),
                pl.col('pais').is_not_null(),
            )
        )
        
        # Import
        df_i = (
            pl.scan_parquet(str(file_import))
            .select([
                'NABAN',
                'PAISGEN',
                'VAFODO',
                'PBK',
                'Data'
            ])
            .rename({
                'NABAN': 'ncm',
                'PAISGEN': 'pais',
                'PBK': 'peso',
                'VAFODO': 'fob',
            })
            .filter(
                pl.col('Data').is_not_null(),
                pl.col('Data').str.contains(r"^\d{4}$")
            )
            .with_columns([
                pl.col('ncm')
                .cast(pl.Utf8)
                .str.strip_chars()
                .str.zfill(10)
                .cast(pl.Categorical),
                pl.col('pais').cast(pl.Categorical),
                pl.col('Data').cast(pl.Utf8),
                pl.col('peso')
                .cast(pl.Utf8)
                .str.strip_chars()
                .str.replace_all(r"\.", "")
                .str.replace_all(",", ".")
                .cast(pl.Float64, strict=False),
                pl.col('fob')
                .cast(pl.Utf8)
                .str.strip_chars()
                .str.replace_all(r"\.", "")
                .str.replace_all(",", ".")
                .cast(pl.Float64, strict=False),
                pl.lit(1).cast(pl.Int8).alias('ImportExport'),  # import
            ])
            .filter(
                pl.col('fob').is_not_null(),
                pl.col('fob') > 0,
                pl.col('peso').is_not_null(),
                pl.col('peso') > 0,
                pl.col('ncm').is_not_null(),
                pl.col('pais').is_not_null(),
            )
        )
        
        # Concatenar e agrupar
        df = (
            pl.concat([df_e, df_i])
            .group_by(['Data', 'ncm', 'ImportExport', 'pais'])
            .agg([
                pl.sum('peso').alias('peso'),
                pl.sum('fob').alias('fob'),
            ])
        )
        
        # Salvar agregado (opcional, para cache)
        df.sink_parquet(
            pl.PartitionByKey(
                base_path,
                by="Data"
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
        Transforma dados da Colômbia: converte Data para dataNCM.
        
        Args:
            lf: LazyFrame com dados agregados
            
        Returns:
            pl.LazyFrame: Dados transformados
        """
        # Converter Data para dataNCM (formato %y%m - 2 dígitos ano)
        lf = lf.with_columns([
            pl.col('Data')
            .str.strptime(pl.Date, "%y%m")
            .alias("dataNCM")
        ])
        
        # Remover Data
        lf = lf.drop(['Data'])
        
        # Renomear pais para idPais
        lf = lf.rename({'pais': 'idPais'})
        
        return lf
    
    def normalize_countries(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Normaliza IDs de países da Colômbia usando arquivo Excel M49.
        
        Args:
            df: DataFrame pandas com dados processados
            
        Returns:
            pd.DataFrame: DataFrame com países normalizados
        """
        # Carregar tabela de países da plataforma
        pais_table_path = self._find_file(self.config['paths']['pais_table'], file_type="tabela de países")
        paisID = pd.read_excel(pais_table_path)
        
        # Carregar arquivo de países M49
        file_paises_path = self._find_file(self.config['paths']['col']['pais_source'], file_type="fonte de países")
        paises = pd.read_excel(file_paises_path)
        paises['idPais'] = paises['Código M49'].astype(str).str.zfill(3)
        paises = paises[['idPais', 'País o área M49 (Español)']]
        paises.rename(columns={'País o área M49 (Español)': 'pais'}, inplace=True)
        
        # Merge com tabela de países da plataforma
        paisID = paisID.merge(paises, left_on='Pais_3', right_on='pais', how='inner')
        paisID = paisID[['idPais', 'IDNCMPais']]
        
        # Merge com dados principais
        df = df.merge(paisID, on='idPais', how='inner')
        df['idPais'] = df['IDNCMPais']
        df = df.drop(columns=['IDNCMPais'])
        
        # Selecionar apenas colunas necessárias
        df = df[['dataNCM', 'idIndicador', 'idPais', 'peso', 'fob']]
        
        return df


def main(config_path: str = 'config_comex.json'):
    """
    Função principal que executa o pipeline completo para Colômbia.
    
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
    print("COMEX COLÔMBIA - Pipeline de Processamento")
    print("=" * 60)
    
    processor = COMEX_COL_GLOBE(config_path)
    results = processor.process()
    
    print(f"\nProcessamento concluído!")
    print(f"Total de batches processados: {len(results)}")
    print(f"Batches com sucesso: {sum(1 for r in results if r.get('status') == 200)}")
    print(f"Batches com erro: {sum(1 for r in results if r.get('status') != 200)}")
    
    return results


if __name__ == "__main__":
    main()

