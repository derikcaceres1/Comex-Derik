"""
Processador COMEX para Argentina.
Migrado de pandas para Polars conforme arquitetura unificada.
"""
import pandas as pd
import polars as pl
from NM.globinho.comex_globe_pipeline import BaseComexProcessor


def unify_columns_polars(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Unifica colunas com formatos diferentes (versão Polars).
    
    Args:
        lf: LazyFrame com dados
        
    Returns:
        pl.LazyFrame: LazyFrame com colunas unificadas
    """
    column_pairs = {
        'fob': ['fob(u$s)', 'fob(usd)'],
        'flete': ['flete(u$s)', 'flete(usd)'],
        'seguro': ['seguro(u$s)', 'seguro(usd)'],
        'cif': ['cif(u$s)', 'cif(usd)']
    }
    
    exprs = []
    cols_to_drop = []
    
    for new_col, cols in column_pairs.items():
        old_col, new_format_col = cols
        # Verificar se ambas colunas existem
        if old_col in lf.columns and new_format_col in lf.columns:
            # Usar coalesce para pegar o primeiro não-nulo
            exprs.append(
                pl.coalesce([pl.col(new_format_col), pl.col(old_col)]).alias(new_col)
            )
            cols_to_drop.extend([old_col, new_format_col])
        elif old_col in lf.columns:
            exprs.append(pl.col(old_col).alias(new_col))
            cols_to_drop.append(old_col)
        elif new_format_col in lf.columns:
            exprs.append(pl.col(new_format_col).alias(new_col))
            cols_to_drop.append(new_format_col)
    
    if exprs:
        lf = lf.with_columns(exprs)
        # Remover colunas antigas
        lf = lf.drop(cols_to_drop)
    
    return lf


def ajust_values_polars(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Ajusta valores: converte tipos, substitui vírgulas, filtra (versão Polars).
    
    Args:
        lf: LazyFrame com dados
        
    Returns:
        pl.LazyFrame: LazyFrame com valores ajustados
    """
    # Converter ncm para numérico e remover nulos
    lf = lf.with_columns([
        pl.col('ncm').cast(pl.Int64, strict=False)
    ])
    lf = lf.filter(pl.col('ncm').is_not_null())
    
    # Converter peso: string -> substituir vírgula -> numérico -> filtrar > 0
    if 'peso' in lf.columns:
        lf = lf.with_columns([
            pl.col('peso')
            .cast(pl.Utf8)
            .str.replace(',', '.')
            .cast(pl.Float64, strict=False)
            .alias('peso')
        ])
        lf = lf.filter(pl.col('peso') > 0)
    
    # Converter fob: string -> substituir vírgula -> numérico
    if 'fob' in lf.columns:
        lf = lf.with_columns([
            pl.col('fob')
            .cast(pl.Utf8)
            .str.replace(',', '.')
            .cast(pl.Float64, strict=False)
            .alias('fob')
        ])
    
    # Converter frete e seguro se existirem
    for col in ['frete', 'seguro']:
        if col in lf.columns:
            lf = lf.with_columns([
                pl.col(col)
                .cast(pl.Utf8)
                .str.replace(',', '.')
                .cast(pl.Float64, strict=False)
                .alias(col)
            ])
    
    return lf


class COMEX_ARG_GLOBE(BaseComexProcessor):
    """Processador COMEX para dados da Argentina."""
    
    def load_raw_data(self) -> pl.LazyFrame:
        """
        Carrega dados brutos da Argentina (import e export) usando Polars.
        
        Returns:
            pl.LazyFrame: Dados brutos concatenados
        """
        file_import_str = self.config['paths']['arg']['import']
        file_export_str = self.config['paths']['arg']['export']
        
        # Encontrar arquivos usando _find_file
        file_import = self._find_file(file_import_str, file_type="de importação")
        file_export = self._find_file(file_export_str, file_type="de exportação")
        
        # Carregar import
        lf_import = pl.scan_parquet(str(file_import))
        lf_import = lf_import.rename({
            'porg': 'pais',
            'pnet(kg)': 'peso',
            'flete': 'frete',
        })
        
        # Converter país para numérico e filtrar
        lf_import = lf_import.with_columns([
            pl.col('pais').cast(pl.Int64, strict=False)
        ])
        lf_import = lf_import.filter(
            (pl.col('pais').is_not_null()) & (pl.col('pais') != 999)
        )
        lf_import = lf_import.select(['data', 'ncm', 'pais', 'peso', 'fob', 'frete', 'seguro'])
        lf_import = ajust_values_polars(lf_import)
        lf_import = lf_import.with_columns([
            pl.lit(1).cast(pl.Int8).alias('ImportExport')
        ])
        
        # Carregar export
        lf_export = pl.scan_parquet(str(file_export))
        lf_export = lf_export.rename({
            'pdes': 'pais',
            'pnet(kg)': 'peso',
        })
        
        # Converter país para numérico e filtrar
        lf_export = lf_export.with_columns([
            pl.col('pais').cast(pl.Int64, strict=False)
        ])
        lf_export = lf_export.filter(
            (pl.col('pais').is_not_null()) & (pl.col('pais') != 999)
        )
        lf_export = lf_export.select(['data', 'ncm', 'pais', 'peso', 'fob'])
        lf_export = ajust_values_polars(lf_export)
        lf_export = lf_export.with_columns([
            pl.lit(0).cast(pl.Int8).alias('ImportExport'),
            # Adicionar colunas frete e seguro como zero para manter schema compatível com import
            pl.lit(0.0).cast(pl.Float64).alias('frete'),
            pl.lit(0.0).cast(pl.Float64).alias('seguro')
        ])
        
        # Unificar colunas se necessário (pode não ser necessário em parquet)
        lf_import = unify_columns_polars(lf_import)
        lf_export = unify_columns_polars(lf_export)
        
        # Garantir que ambos tenham as mesmas colunas na mesma ordem antes de concatenar
        common_cols = ['data', 'ncm', 'pais', 'peso', 'fob', 'frete', 'seguro', 'ImportExport']
        lf_import = lf_import.select(common_cols)
        lf_export = lf_export.select(common_cols)
        
        # Concatenar
        lf = pl.concat([lf_import, lf_export])
        print(f"COMEX ARGENTINA: Dados brutos carregados com {lf.tail(10).collect()} registros.")
        
        return lf
    
    def transform_data(self, lf: pl.LazyFrame) -> pl.LazyFrame:
        """
        Transforma dados da Argentina: renomeia data para dataNCM.
        
        Args:
            lf: LazyFrame com dados brutos
            
        Returns:
            pl.LazyFrame: Dados transformados
        """
        # Renomear data para dataNCM
        lf = lf.rename({'data': 'dataNCM'})
        
        # Renomear pais para idPais
        lf = lf.rename({'pais': 'idPais'})
        
        return lf
    
    def normalize_countries(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Normaliza IDs de países da Argentina usando tabela de mapeamento.
        
        Args:
            df: DataFrame pandas com dados processados
            
        Returns:
            pd.DataFrame: DataFrame com países normalizados
        """
        # Carregar tabela de países da plataforma
        pais_table_path = self._find_file(self.config['paths']['pais_table'], file_type="tabela de países")
        paisID = pd.read_excel(pais_table_path)
        
        # Carregar tabela de países da fonte
        pais_source_path = self._find_file(self.config['paths']['arg']['pais_source'], file_type="fonte de países")
        paisID_source = pd.read_csv(
            pais_source_path,
            sep=';'
        )
        
        # Fazer merge para normalizar
        paisID_normalized = paisID_source.merge(
            paisID,
            left_on='nombre',
            right_on='Pais_3',
            how='inner'
        )
        paisID_normalized = paisID_normalized[['_id', 'IDNCMPais']]
        paisID_normalized.rename(columns={'_id': 'idPais'}, inplace=True)
        
        # Merge com dados principais
        df = df.merge(paisID_normalized, on='idPais', how='inner')
        df['idPais'] = df['IDNCMPais']
        df = df.drop(columns=['IDNCMPais'])
        
        print('Após normalização de países, dados:\n', df.head())
        return df


def main(config_path: str = 'config_comex.json'):
    """
    Função principal que executa o pipeline completo para Argentina.
    
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
    print("COMEX ARGENTINA - Pipeline de Processamento")
    print("=" * 60)
    
    processor = COMEX_ARG_GLOBE(config_path)
    results = processor.process()
    
    print(f"\nProcessamento concluído!")
    print(f"Total de batches processados: {len(results)}")
    print(f"Batches com sucesso: {sum(1 for r in results if r.get('status') == 200)}")
    print(f"Batches com erro: {sum(1 for r in results if r.get('status') != 200)}")
    
    return results


if __name__ == "__main__":
    main()

