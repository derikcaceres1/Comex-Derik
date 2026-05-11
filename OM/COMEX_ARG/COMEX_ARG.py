import io
import time
import sys
import os
from pathlib import Path
from datetime import datetime
from zipfile import ZipFile
from typing import Tuple
import pandas as pd
import requests

# Adicionar path do diretório OM ao sys.path para imports
project_root = Path(__file__).parent.parent.parent
om_path = project_root / "OM"
if str(om_path) not in sys.path:
    sys.path.insert(0, str(om_path))

from OM.costdrivers_comex_OM import ComexPipeline


class COMEX_ARG(ComexPipeline):
    """Pipeline de COMEX específico para Argentina."""
    
    def __init__(self, start_date=None, use_azure=True):
        super().__init__(
            iso_code='ARG',
            start_date=start_date,
            data_contract_path='data-contract.yaml',
            ids_table_path='IDS_comex.xlsx',
            use_azure=use_azure
        )
        self.base_url = "https://comex.indec.gob.ar/files/zips"
        # numero de anos para coletar dados
        self.n_years = 1
    
    def _get_country_name(self) -> str:
        """Retorna nome do país para filtro na tabela de IDs."""
        return 'Argentina'
    
    def _make_zip_url(self, ie_type: str, year: str) -> str:
        """Cria URL para download do arquivo zip."""
        return f"{self.base_url}/{ie_type}s_{year}_M.zip"
    
    def _download_zip(self, url: str) -> bytes:
        """Baixa arquivo zip da URL."""
        max_retries = 3
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                self.logger.info(f"Tentando download: {url}")
                req = requests.get(url, timeout=30)
                
                if req.status_code == 200:
                    with ZipFile(io.BytesIO(req.content), "r") as mzip:
                        # Procurar arquivo com 'expopm' no nome
                        if any("expopm" in name for name in mzip.namelist()):
                            filename = next(name for name in mzip.namelist() if "expopm" in name)
                            self.logger.info(f"Arquivo encontrado: {filename}")
                            return mzip.read(filename)
                        
                        # Caso contrário, pegar o maior arquivo
                        files_sizes = [(info.file_size, info.filename) for info in mzip.infolist()]
                        if files_sizes:
                            largest_file = max(files_sizes, key=lambda x: x[0])[1]
                            self.logger.info(f"Maior arquivo encontrado: {largest_file}")
                            return mzip.read(largest_file)
                
                elif req.status_code == 404:
                    self.logger.warning(f"Arquivo não encontrado (404): {url}")
                    return None
                else:
                    self.logger.warning(f"Status code {req.status_code} para {url}")
                    
            except Exception as e:
                retry_count += 1
                self.logger.error(f"Erro no download (tentativa {retry_count}/{max_retries}): {str(e)}")
                if retry_count < max_retries:
                    time.sleep(10)
                else:
                    return None
        
        return None
    
    def _bytes_to_dataframe(self, csv_bytes: bytes, ie_type: str) -> pd.DataFrame:
        """Converte bytes CSV em DataFrame."""
        df = pd.read_csv(io.BytesIO(csv_bytes), sep=";", encoding="latin-1", dtype=str)
        df.columns = [col.lower() for col in df.columns]
        
        # Criar coluna de data
        df["data"] = df.apply(lambda x: f"01-{x['mes'].zfill(2)}-{x['año']}", axis=1)
        df["data"] = pd.to_datetime(df["data"], format="%d-%m-%Y")
        
        self.logger.info(f"DataFrame criado: {len(df)} registros")
        return df
    
    def _process_ie_type(self, ie_type: str) -> pd.DataFrame:
        """
        Processa dados de importação ou exportação.
        
        Args:
            ie_type: 'import' ou 'export'
        """
        dfs = []
        
        start_date = datetime.now() - pd.DateOffset(years=self.n_years)
        
        self.period_dates = [date for date in pd.date_range(start_date, datetime.now(), freq='MS')]
        years = list(set([d.year for d in self.period_dates]))
        
        for year in years:
            self.logger.info(f"Processando {ie_type.upper()} - Ano {year}")
            
            url = self._make_zip_url(ie_type, str(year))
            csv_bytes = self._download_zip(url)
            
            if csv_bytes is None:
                self.logger.warning(f"Sem dados para {ie_type} no ano {year}")
                continue
            
            df = self._bytes_to_dataframe(csv_bytes, ie_type)
            
            # Salvar parquet intermediário (opcional)
            # df.to_parquet(f'temp_comex_argentina_{ie_type}_{year}.parquet', index=False)
            
            dfs.append(df)
        
        if not dfs:
            self.logger.warning(f"Nenhum dado coletado para {ie_type}")
            return pd.DataFrame()
        
        result_df = pd.concat(dfs, ignore_index=True)
        self.logger.info(f"Total {ie_type}: {len(result_df)} registros")
        return result_df
    
    def collect_import_data(self) -> pd.DataFrame:
        """Coleta dados de importação da Argentina."""
        return self._process_ie_type("import")
    
    def collect_export_data(self) -> pd.DataFrame:
        """Coleta dados de exportação da Argentina."""
        return self._process_ie_type("export")
    
    def _country_specific_treatment(self, df: pd.DataFrame) -> pd.DataFrame:
        """Tratamento específico para dados da Argentina."""
        # Criar coluna data a partir de ano e mês se necessário
        if 'data' not in df.columns and 'año' in df.columns and 'mes' in df.columns:
            df['data'] = pd.to_datetime({
                'year': df['año'].astype(int),
                'month': df['mes'].astype(int),
                'day': 1
            })
        
        return df

    def atualizar_historico(self) -> None:
        """
        Atualiza os arquivos históricos de cada país europeu com dados novos.
        
        Este método deve ser chamado após collect() ter sido executado,
        pois depende de self.raw_import_df e self.raw_export_df.
        
        out:
            export_hist_arg_raw.parquet
            import_hist_arg_raw.parquet

        """
        
        self.logger.info("=== INICIANDO atualizar_historico ===")
        
        path_dados = os.path.join("NM", "dados", "ARG", "database")
        
        arquivo_IE = {
            "export": "export_hist_arg_raw.parquet",
            "import": "import_hist_arg_raw.parquet"
        }
        
        # 1. Verificar se os DataFrames de import/export existem
        if not hasattr(self, 'raw_import_df') or not hasattr(self, 'raw_export_df'):
            self.logger.error("raw_import_df ou raw_export_df não encontrados. Execute collect() primeiro.")
            return
        
        if self.raw_import_df.empty and self.raw_export_df.empty:
            self.logger.error("raw_import_df e raw_export_df estão vazios. Execute collect() primeiro.")
            return
        
        df_historic = self.raw_export_df.copy()
        
        # 2. Carregar histórico do export para comparação de datas
        ref_path = os.path.join(path_dados, arquivo_IE["export"])
        
        try:
            df_database = pd.read_parquet(ref_path)
            self.logger.info(f"Histórico de referência carregado: {len(df_database)} registros")
        except FileNotFoundError:
            raise FileNotFoundError(f"Arquivo histórico de referência não encontrado: {ref_path}")
        
        # 3. Comparar as datas para verificar se há dados novos
        if 'data' not in df_historic.columns:
            self.logger.error("Coluna data não encontrada nos dados. Abortando.")
            return
        
        # Converter data para datetime se necessário
        max_period_new = df_historic['data'].max()
        
        if not df_database.empty and 'data' in df_database.columns:
            # Garantir que ambos são inteiros para comparação
            df_database['data'] = pd.to_datetime(df_database['data'])
            max_period_db = df_database['data'].max()
            self.logger.info(f"Período máximo no histórico: {max_period_db}")
            self.logger.info(f"Período máximo nos dados novos: {max_period_new}")
            
            if max_period_new <= max_period_db:
                self.logger.info("Nenhum dado novo encontrado. Históricos já estão atualizados.")
                return
            
            self.logger.info(f"Dados novos detectados! Período {max_period_db} -> {max_period_new}")
        else:
            self.logger.info(f"Criando históricos novos. Período máximo: {max_period_new}")
        
        for ie_type, filename in arquivo_IE.items():
            self.logger.info(f"=== atualizar_historico: {ie_type} ===")
            
            # dados coletados
            if ie_type == "export":
                df_country = self.raw_export_df.copy()
            else:
                df_country = self.raw_import_df.copy()
            
            # filtrar apenas dados novos
            df_country = df_country[df_country['data'] >= max_period_db]
            df_country.rename(columns={'fob(usd)': 'fob'}, inplace=True)
            
            # 
            if ie_type == "import":
                df_country.rename(columns={'flete(usd)': 'flete', 'seguro(usd)': 'seguro', 'cif(usd)': 'cif'}, inplace=True)
            
            print('df_country:\n', df_country.head())
            print(df_country.info())
             
            # Caminho do arquivo histórico do país
            historical_path = os.path.join(path_dados, filename)
            
            # Carregar histórico existente (se houver)
            try:
                df_existing = pd.read_parquet(historical_path)
                self.logger.info(f"[{ie_type}] Histórico existente carregado: {len(df_existing)} registros")
            except FileNotFoundError:
                self.logger.info(f"[{ie_type}] Arquivo histórico não encontrado. Criando novo.")
                df_existing = pd.DataFrame()
            except Exception as e:
                self.logger.error(f"[{ie_type}] Erro ao carregar histórico: {str(e)}")
                continue
            
            print('df_existing:\n', df_existing.head())
            print(df_existing.info())
            
            # filtrar para as colunas que usamos
            historical_columns = df_existing.columns
            df_country = df_country[historical_columns]
            
            # validar que existam todas as colunas
            missing_cols = set(historical_columns) - set(df_country.columns)
            if missing_cols:
                self.logger.error(f"[{ie_type}] Colunas faltando nos dados coletados: {missing_cols}. Abortando atualização.")
                continue
            
            # concatenando dados novos com existentes
            df_updated = pd.concat([df_existing, df_country], ignore_index=True)
            
            # garantir que não há duplicatas
            subset_columns = ['data', 'ncm', 'pnet(kg)']
            if ie_type == 'export':
                subset_columns.append('pdes')
            else:
                import_cols = ['porg', 'cif']
                subset_columns.extend(import_cols)
            
            df_updated.drop_duplicates(subset=subset_columns, keep='last', inplace=True)
            
            # Salvar arquivo atualizado
            try:
                df_updated.to_parquet(historical_path, index=False, engine='pyarrow')
                self.logger.info(f"[{ie_type}] Histórico atualizado: {len(df_updated)} registros salvos em {historical_path}")
            except Exception as e:
                self.logger.error(f"[{ie_type}] Erro ao salvar histórico: {str(e)}")
        
        # 7. Resumo final
        self.logger.info("=" * 50)
        self.logger.info("RESUMO DA ATUALIZAÇÃO DE HISTÓRICOS")
        self.logger.info("=" * 50)

    def collect(self) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Executa a fase de coleta completa.
        Usa cache local se disponível e válido (apenas em ambiente de desenvolvimento).
        
        Returns:
            Tupla (import_df, export_df)
        """
        self.logger.info("=== INICIANDO FASE 1: COLETA ===")
        
        # Verificar cache para importação
        cached_import = self._load_from_cache('import')
        if cached_import is not None:
            self.logger.info("Usando cache para dados de IMPORTAÇÃO")
            self.raw_import_df = cached_import
        else:
            # Coletar importação
            self.logger.info("Coletando dados de IMPORTAÇÃO...")
            self.raw_import_df = self.collect_import_data()
            # Salvar no cache se em ambiente local
            self._save_to_cache(self.raw_import_df, 'import')
        
        if not self.raw_import_df.empty:
            self.logger.info(f"Importação coletada: {len(self.raw_import_df)} registros")
            date_str = datetime.now().strftime('%Y-%m-%d')
            self._save_to_storage(self.raw_import_df, self.raw_path, f"import_raw_{date_str}.parquet")
        else:
            self.logger.warning("Nenhum dado de importação coletado")
        
        # Verificar cache para exportação
        cached_export = self._load_from_cache('export')
        if cached_export is not None:
            self.logger.info("Usando cache para dados de EXPORTAÇÃO")
            self.raw_export_df = cached_export
        else:
            # Coletar exportação
            self.logger.info("Coletando dados de EXPORTAÇÃO...")
            self.raw_export_df = self.collect_export_data()
            # Salvar no cache se em ambiente local
            self._save_to_cache(self.raw_export_df, 'export')
        
        if not self.raw_export_df.empty:
            self.logger.info(f"Exportação coletada: {len(self.raw_export_df)} registros")
            date_str = datetime.now().strftime('%Y-%m-%d')
            self._save_to_storage(self.raw_export_df, self.raw_path, f"export_raw_{date_str}.parquet")
        else:
            self.logger.warning("Nenhum dado de exportação coletado")
        
        self.atualizar_historico()
        
        self.logger.info("=== FASE 1 CONCLUÍDA ===\n")
        return self.raw_import_df, self.raw_export_df

def main():
    """Execução local do pipeline completo."""
    # Criar instância do pipeline
    pipeline = COMEX_ARG(start_date=None)  # ou datetime(2024, 1, 1)
    
    # Executar pipeline completo
    result_df = pipeline.run()
    
    # Salvar resultado final localmente
    # timestamp = datetime.now().strftime('%Y%m%dT%H%M')
    # result_df.to_excel(f'subir_COMEX_ARG_{timestamp}.xlsx', index=False)
    
    return result_df


if __name__ == "__main__":
    result = main()
    print(f"\nPipeline concluído! Total de registros: {len(result)}")