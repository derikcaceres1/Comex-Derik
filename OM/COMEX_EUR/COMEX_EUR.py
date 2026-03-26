import os
import shutil
import sys
from datetime import datetime, timedelta
from io import BytesIO
from typing import List, Dict, Optional, Tuple
from pathlib import Path
import pandas as pd
import requests
from bs4 import BeautifulSoup
import warnings

# Adicionar path do diretório OM ao sys.path para imports
project_root = Path(__file__).parent.parent.parent
om_path = project_root / "OM"
if str(om_path) not in sys.path:
    sys.path.insert(0, str(om_path))

from OM.costdrivers_comex_OM import ComexPipeline

# Desativar avisos
warnings.filterwarnings("ignore", category=UserWarning, module="bs4")
requests.packages.urllib3.disable_warnings(requests.packages.urllib3.exceptions.InsecureRequestWarning)


class COMEX_EUR(ComexPipeline):
    """Pipeline de COMEX específico para Europa (Eurostat)."""
    
    def __init__(self, start_date=None, use_azure=True, last_months=3):
        super().__init__(
            iso_code='EUR',
            start_date=start_date,
            data_contract_path='include/data-contract.yaml',
            ids_table_path='library/IDS_comex.xlsx',
            use_azure=use_azure
        )
        self.base_url = "https://ec.europa.eu/eurostat/api/dissemination/files/?sort=1&dir=comext%2FCOMEXT_DATA%2FPRODUCTS"
        self.temp_folder = None
        self.last_months = last_months
    
    def _get_country_name(self) -> str:
        """Retorna nome do país para filtro na tabela de IDs."""
        return 'União Européia'
    
    def _create_temp_folder(self) -> Path:
        """Cria pasta temporária para extração de arquivos."""
        folder_path = Path(".temp") / "eurostat"
        folder_path.mkdir(parents=True, exist_ok=True)
        self.temp_folder = folder_path
        return folder_path
    
    def _cleanup_temp_folder(self):
        """Remove pasta temporária após uso."""
        if self.temp_folder and self.temp_folder.exists():
            try:
                shutil.rmtree(self.temp_folder)
                self.logger.info(f"Pasta temporária removida: {self.temp_folder}")
            except Exception as e:
                self.logger.warning(f"Erro ao remover pasta temporária: {str(e)}")
    
    def _get_available_files(self) -> List[Dict[str, any]]:
        """
        Retorna lista de arquivos .7z disponíveis no portal Eurostat.
        
        Returns:
            Lista de dicionários com informações dos arquivos (url, data, nome, tamanho)
        """
        try:
            self.logger.info("Acessando portal Eurostat...")
            response = requests.get(self.base_url, verify=False ,timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, "lxml")
            table = soup.find("table")
            
            if not table:
                self.logger.error("Tabela não encontrada no portal Eurostat")
                return []
            
            # Ler tabela HTML
            df_links = pd.read_html(str(table))[0]
            
            # Filtrar apenas arquivos .7z
            df_links = df_links.loc[df_links["Type"] == "7z"].copy()
            
            if df_links.empty:
                self.logger.warning("Nenhum arquivo .7z encontrado")
                return []
            
            # Converter tamanho para float (ex: "123.4 MB" -> 123.4)
            df_links["Size"] = df_links["Size"].apply(
                lambda x: float(x.split(" ")[0]) if pd.notna(x) else 0.0
            )
            
            # Extrair data do nome do arquivo (formato: full_v2_YYYYMM)
            df_links["Date"] = df_links["Name"].apply(
                lambda x: x.split(".")[0].replace("full_v2_", "") if pd.notna(x) else ""
            )
            
            # Filtrar apenas datas válidas (6 dígitos YYYYMM)
            df_links = df_links[df_links["Date"].apply(lambda x: len(str(x)) == 6)]
            
            # Excluir semanas 52 (dados anuais)
            df_links = df_links.loc[~df_links["Date"].str.contains("52", na=False)]
            
            # Converter data para formato datetime (YYYYMM -> 01-MM-YYYY)
            df_links["Date"] = df_links["Date"].apply(
                lambda x: pd.to_datetime(f"01-{x[4:]}-{x[:4]}", format="%d-%m-%Y") if len(x) == 6 else None
            )
            
            # Obter links dos arquivos
            links = soup.find_all("a")
            df_links["Links"] = df_links["Name"].apply(
                lambda x: (
                    f"https://ec.europa.eu/{[y for y in links if x in y.text and 'xixu' not in y.text][0]['href']}"
                    if any(x in y.text and 'xixu' not in y.text for y in links) and 
                       [y for y in links if x in y.text and 'xixu' not in y.text][0].get('href')
                    else None
                )
            )
            
            # Remover linhas sem link válido
            df_links = df_links[df_links["Links"].notna()].copy()
            
            # Filtrar arquivos com tamanho > 20 MB
            df_links = df_links.loc[df_links["Size"] > 20]
            
            # Ordenar por data (mais recente primeiro)
            df_links = df_links.sort_values(by=["Date"], ascending=False)
            
            # Converter para lista de dicionários
            files = []
            for _, row in df_links.iterrows():
                files.append({
                    "url": row["Links"],
                    "name": row["Name"],
                    "date": row["Date"],
                    "size": row["Size"]
                })
            
            self.logger.info(f"Encontrados {len(files)} arquivos .7z no portal Eurostat")
            return files
            
        except requests.RequestException as e:
            self.logger.error(f"Erro ao acessar portal Eurostat: {str(e)}")
            return []
        except Exception as e:
            self.logger.error(f"Erro ao processar arquivos do Eurostat: {str(e)}")
            return []
    
    def _download_and_extract(self, file_info: Dict[str, any]) -> Optional[Path]:
        """
        Baixa arquivo .7z e extrai arquivo .dat.
        
        Args:
            file_info: Dicionário com informações do arquivo (url, name, date, size)
            
        Returns:
            Caminho do arquivo .dat extraído, ou None em caso de erro
        """
        try:
            # Criar pasta temporária se não existir
            if not self.temp_folder:
                self._create_temp_folder()
            
            file_name_7z = file_info["name"]
            file_name_dat = file_name_7z.replace(".7z", ".dat").replace("_v2_", "_")
            
            file_path_7z = self.temp_folder / file_name_7z
            file_path_dat = self.temp_folder / file_name_dat
            
            # Verificar se arquivo .dat já existe
            if file_path_dat.exists():
                self.logger.info(f"Arquivo .dat já existe: {file_name_dat}")
                return file_path_dat
            
            # Verificar se arquivo .7z já existe
            if file_path_7z.exists():
                self.logger.info(f"Arquivo .7z já existe, extraindo: {file_name_7z}")
            else:
                # Baixar arquivo .7z
                self.logger.info(f"Baixando arquivo: {file_name_7z}")
                response = requests.get(file_info["url"], stream=True, verify=False,timeout=300)
                response.raise_for_status()
                
                # Salvar arquivo .7z
                with open(file_path_7z, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)
                
                self.logger.info(f"Arquivo baixado: {file_name_7z}")
            
            # Extrair arquivo .dat do .7z
            try:
                import py7zr
                self.logger.info(f"Extraindo arquivo .dat de {file_name_7z}...")
                
                with py7zr.SevenZipFile(file_path_7z, mode='r') as archive:
                    archive.extractall(path=self.temp_folder)
                
                # Verificar se arquivo .dat foi extraído
                if file_path_dat.exists():
                    self.logger.info(f"Arquivo .dat extraído: {file_name_dat}")
                    return file_path_dat
                else:
                    # Tentar encontrar arquivo .dat com nome alternativo
                    dat_files = list(self.temp_folder.glob("*.dat"))
                    if dat_files:
                        self.logger.info(f"Arquivo .dat encontrado com nome alternativo: {dat_files[0].name}")
                        return dat_files[0]
                    else:
                        self.logger.error(f"Arquivo .dat não encontrado após extração de {file_name_7z}")
                        return None
                        
            except ImportError:
                self.logger.error("Biblioteca py7zr não encontrada. Instale com: pip install py7zr")
                return None
            except Exception as e:
                self.logger.error(f"Erro ao extrair arquivo .7z: {str(e)}")
                return None
                
        except requests.RequestException as e:
            self.logger.error(f"Erro ao baixar arquivo {file_info.get('name', 'unknown')}: {str(e)}")
            return None
        except Exception as e:
            self.logger.error(f"Erro ao processar arquivo: {str(e)}")
            return None
    
    def _process_dat_file(self, dat_path: Path) -> pd.DataFrame:
        """
        Processa arquivo .dat completo (import + export juntos) e retorna DataFrame.
        
        Args:
            dat_path: Caminho do arquivo .dat
            
        Returns:
            DataFrame processado com todos os dados (import e export)
        """
        try:
            self.logger.info(f"Processando arquivo .dat: {dat_path.name}")
            
            # Ler arquivo .dat
            df = pd.read_csv(
                dat_path,
                sep=",",
                encoding="iso-8859-1",
                dtype=str,
                low_memory=False
            )
            
            # Filtrar linhas onde NCM != "TOTAL"
            if 'PRODUCT_NC' in df.columns:
                df = df[df['PRODUCT_NC'] != "TOTAL"].copy()
            
            # Converter PERIOD (YYYYMM) para Data, mas manter PERIOD para uso posterior
            df['Data'] = pd.to_datetime(
                df['PERIOD'].astype(str),
                format='%Y%m',
                errors='coerce'
            )
            # Não dropar PERIOD aqui, será usado em _collect_all_data() para seleção de colunas
            
            # Adicionar colunas frete e seguro com valor 0 (EUR não tem esses dados)
            df['frete'] = 0.0
            df['seguro'] = 0.0
            
            self.logger.info(f"Arquivo processado: {len(df)} registros (import + export)")
            return df
            
        except Exception as e:
            self.logger.error(f"Erro ao processar arquivo .dat {dat_path.name}: {str(e)}")
            return pd.DataFrame()
    
    def _collect_all_data(self) -> pd.DataFrame:
        """
        Coleta todos os dados (import + export) de uma vez.
        Sempre baixa os 3 arquivos mais recentes, processa e retorna tudo junto.
        Salva os dados por ano durante a coleta para evitar esgotamento de memória.
        
        Returns:
            DataFrame com todos os dados (import e export juntos)
        """
        # Obter arquivos disponíveis (já ordenados por data, mais recente primeiro)
        files = self._get_available_files()
        
        if not files:
            self.logger.warning("Nenhum arquivo encontrado")
            return pd.DataFrame()
        
        # Pegar apenas os 3 arquivos mais recentes (posições 0 e 1)
        files_to_process = files[:self.last_months]
        
        if len(files_to_process) < self.last_months:
            self.logger.warning(f"Apenas {len(files_to_process)} arquivo(s) disponível(is), processando o(s) disponível(is)")
        
        self.logger.info(f"Processando os {len(files_to_process)} arquivo(s) mais recente(s) (import + export juntos)")
        for i, file_info in enumerate(files_to_process):
            self.logger.info(f"  Arquivo {i+1}: {file_info['name']} ({file_info['date'].strftime('%Y-%m')})")
        
        dfs = []
        
        try:
            # Processar cada arquivo (sem filtrar por tipo)
            for file_info in files_to_process:
                file_year = file_info['date'].year
                self.logger.info(f"Processando arquivo completo - {file_info['date'].strftime('%Y-%m')} (ano: {file_year})")
                
                # Baixar e extrair arquivo .dat
                dat_path = self._download_and_extract(file_info)
                
                if dat_path is None:
                    self.logger.warning(f"Erro ao baixar/extrair arquivo em {file_info['date'].strftime('%Y-%m')}")
                    continue
                
                # Processar arquivo .dat completo (sem filtrar por trade_type)
                df = self._process_dat_file(dat_path)
                
                # Selecionar colunas necessárias, incluindo coluna de tipo de comércio se existir
                use_cols = ['PRODUCT_NC', 'REPORTER', 'PARTNER', 'VALUE_EUR', 'QUANTITY_KG', 'PERIOD']
                # Adicionar coluna de tipo de comércio se existir (necessária para separação import/export)
                if 'TRADE_TYPE' in df.columns:
                    use_cols.append('TRADE_TYPE')
                elif 'FLOW' in df.columns:
                    use_cols.append('FLOW')
                # Manter apenas colunas que existem no DataFrame
                use_cols = [col for col in use_cols if col in df.columns]
                df = df[use_cols]
                
                if not df.empty:
                    dfs.append(df)
                    self.logger.info(f"DataFrame criado: {len(df)} registros (import + export) - {file_info['date'].strftime('%Y-%m')}")
                else:
                    self.logger.warning(f"Nenhum dado processado em {file_info['date'].strftime('%Y-%m')}")
            
            df = pd.concat(dfs)
            
            if not df.empty:
                self.logger.info(f"DataFrames concatenados: {len(df)} registros (import + export)")
                return df
            else:
                self.logger.info("Nenhum dado coletado após processamento")
                return pd.DataFrame()
            
        finally:
            pass
    
    def collect(self) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Executa a fase de coleta completa.
        Coleta todos os dados de uma vez (import + export juntos) e depois separa.
        Usa cache local se disponível e válido (apenas em ambiente de desenvolvimento).
        
        Returns:
            Tupla (import_df, export_df)
        """
        self.logger.info("=== INICIANDO FASE 1: COLETA ===")
        
        # Verificar cache para dados completos
        cached_all = self._load_from_cache('all')
        if cached_all is not None:
            self.logger.info("Usando cache para dados completos (import + export)")
            df_all = cached_all
        else:
            # Coletar todos os dados de uma vez
            self.logger.info("Coletando dados completos (import + export juntos)...")
            df_all = self._collect_all_data()
            # Salvar no cache se em ambiente local
            if not df_all.empty:
                self._save_to_cache(df_all, 'all')
        
        if df_all.empty:
            self.logger.warning("Nenhum dado coletado")
            self.raw_import_df = pd.DataFrame()
            self.raw_export_df = pd.DataFrame()
            self.logger.info("=== FASE 1 CONCLUÍDA ===\n")
            return self.raw_import_df, self.raw_export_df
        
        # Separar em import e export
        # Verificar qual coluna identifica o tipo de comércio
        if 'TRADE_TYPE' in df_all.columns:
            # TRADE_TYPE: 'I' = importação, 'E' = exportação
            self.raw_import_df = df_all[df_all['TRADE_TYPE'] == 'I'].copy()
            self.raw_export_df = df_all[df_all['TRADE_TYPE'] == 'E'].copy()
        elif 'FLOW' in df_all.columns:
            # FLOW: 1 = importação, 2 = exportação
            self.raw_import_df = df_all[df_all['FLOW'] == '1'].copy()
            self.raw_export_df = df_all[df_all['FLOW'] == '2'].copy()
        else:
            self.logger.error("Coluna TRADE_TYPE ou FLOW não encontrada para separar import/export")
            self.raw_import_df = pd.DataFrame()
            self.raw_export_df = pd.DataFrame()
            self.logger.info("=== FASE 1 CONCLUÍDA ===\n")
            return self.raw_import_df, self.raw_export_df
        
        # Salvar separadamente no ADLS
        if not self.raw_import_df.empty:
            self.logger.info(f"Importação separada: {len(self.raw_import_df)} registros")
            date_str = datetime.now().strftime('%Y-%m-%d')
            self._save_to_storage(self.raw_import_df, self.raw_path, f"import_raw_{date_str}.parquet")
        else:
            self.logger.warning("Nenhum dado de importação após separação")
        
        if not self.raw_export_df.empty:
            self.logger.info(f"Exportação separada: {len(self.raw_export_df)} registros")
            date_str = datetime.now().strftime('%Y-%m-%d')
            self._save_to_storage(self.raw_export_df, self.raw_path, f"export_raw_{date_str}.parquet")
        else:
            self.logger.warning("Nenhum dado de exportação após separação")
        
        self.atualizar_historico()
        
        self.logger.info("=== FASE 1 CONCLUÍDA ===\n")
        return self.raw_import_df, self.raw_export_df
    
    def collect_import_data(self) -> pd.DataFrame:
        """Coleta dados de importação da Europa."""
        # Este método não é mais usado diretamente, mas mantido para compatibilidade
        # A coleta é feita através do método collect() que coleta tudo junto
        return pd.DataFrame()
    
    def collect_export_data(self) -> pd.DataFrame:
        """Coleta dados de exportação da Europa."""
        # Este método não é mais usado diretamente, mas mantido para compatibilidade
        # A coleta é feita através do método collect() que coleta tudo junto
        return pd.DataFrame()
    
    def run(self, skip_phases: List[str] = None) -> pd.DataFrame:
        """
        Executa o pipeline completo com limpeza de pasta temporária.
        Sobrescreve o método run() da classe base para garantir limpeza.
        """
        try:
            result = super().run(skip_phases=skip_phases)
            return result
        finally:
            # Limpar pasta temporária após execução completa
            self._cleanup_temp_folder()
    
    def atualizar_historico(self) -> None:
        """
        Atualiza os arquivos históricos de cada país europeu com dados novos.
        
        Este método deve ser chamado após collect() ter sido executado,
        pois depende de self.raw_import_df e self.raw_export_df.
        
        Fluxo:
        1. Concatena raw_import_df e raw_export_df
        2. Carrega histórico de referência (BGR)
        3. Compara datas para verificar se há dados novos
        4. Filtra por REPORTER nos países de interesse
        5. Para cada país, atualiza o arquivo historical.parquet
        """
        # Mapeamento ISO-2 para ISO-3
        iso_map = {
            'AT': 'AUT',
            'BE': 'BEL',
            'BG': 'BGR',
            'DE': 'DEU',
            'FR': 'FRA',
            'HU': 'HUN',
            'IT': 'ITA',
            'NL': 'NLD',
            'PT': 'PRT',
            'EUR': 'EUR',
        }
        
        # 1. Verificar se os DataFrames de import/export existem
        if not hasattr(self, 'raw_import_df') or not hasattr(self, 'raw_export_df'):
            self.logger.error("raw_import_df ou raw_export_df não encontrados. Execute collect() primeiro.")
            return
        
        if self.raw_import_df.empty and self.raw_export_df.empty:
            self.logger.error("raw_import_df e raw_export_df estão vazios. Execute collect() primeiro.")
            return
        
        # 2. Concatenar import e export em um único DataFrame
        self.logger.info("Concatenando dados de importação e exportação...")
        df_historic = pd.concat([self.raw_import_df, self.raw_export_df], ignore_index=True)
        self.logger.info(f"Total de registros concatenados: {len(df_historic)}")
        
        if df_historic.empty:
            self.logger.warning("DataFrame concatenado está vazio. Nada a atualizar.")
            return
        
        # 3. Carregar histórico de referência (BGR) para comparação de datas
        ref_path = os.path.join("NM", "dados", "BGR", "database", "historical.parquet")
        
        try:
            df_database = pd.read_parquet(ref_path)
            self.logger.info(f"Histórico de referência carregado: {len(df_database)} registros")
        except FileNotFoundError:
            self.logger.warning(f"Arquivo de referência não encontrado: {ref_path}. Criando históricos novos.")
            df_database = pd.DataFrame()
        except Exception as e:
            self.logger.error(f"Erro ao carregar histórico de referência: {str(e)}")
            return
        
        # 4. Comparar datas (PERIOD) para verificar se há dados novos
        if 'PERIOD' not in df_historic.columns:
            self.logger.error("Coluna PERIOD não encontrada nos dados. Abortando.")
            return
        
        # Converter PERIOD para inteiro se necessário
        df_historic['PERIOD'] = pd.to_numeric(df_historic['PERIOD'], errors='coerce').astype('Int64')
        max_period_new = int(df_historic['PERIOD'].max())
        
        if not df_database.empty and 'PERIOD' in df_database.columns:
            # Garantir que ambos são inteiros para comparação
            df_database['PERIOD'] = pd.to_numeric(df_database['PERIOD'], errors='coerce')
            max_period_db = int(df_database['PERIOD'].max())
            self.logger.info(f"Período máximo no histórico: {max_period_db}")
            self.logger.info(f"Período máximo nos dados novos: {max_period_new}")
            
            if max_period_new <= max_period_db:
                self.logger.info("Nenhum dado novo encontrado. Históricos já estão atualizados.")
                return
            
            self.logger.info(f"Dados novos detectados! Período {max_period_db} -> {max_period_new}")
        else:
            self.logger.info(f"Criando históricos novos. Período máximo: {max_period_new}")
        
        # 5. Filtrar apenas os países de interesse
        valid_reporters = list(iso_map.keys())
        df_filtered = df_historic[df_historic['REPORTER'].isin(valid_reporters)].copy()
        
        if df_filtered.empty:
            self.logger.warning(f"Nenhum dado encontrado para os países: {valid_reporters}")
            return
        
        self.logger.info(f"Registros filtrados para países de interesse: {len(df_filtered)}")
        
        # 6. Atualizar histórico de cada país
        countries_updated = []
        countries_skipped = []
        
        for iso2, iso3 in iso_map.items():
            if iso3 != 'EUR':
                # Filtrar dados do país
                df_country = df_filtered[df_filtered['REPORTER'] == iso2].copy()
            else:
                df_country = df_filtered.copy()
            
            if df_country.empty:
                self.logger.warning(f"[{iso3}] Nenhum dado encontrado para REPORTER={iso2}")
                countries_skipped.append(iso3)
                continue
            
            # Caminho do arquivo histórico do país
            hist_path = os.path.join("NM", "dados", iso3, "database", "historical.parquet")
            hist_dir = os.path.dirname(hist_path)
            
            # Carregar histórico existente (se houver)
            try:
                df_existing = pd.read_parquet(hist_path)
                self.logger.info(f"[{iso3}] Histórico existente carregado: {len(df_existing)} registros")
            except FileNotFoundError:
                self.logger.info(f"[{iso3}] Arquivo histórico não encontrado. Criando novo.")
                df_existing = pd.DataFrame()
            except Exception as e:
                self.logger.error(f"[{iso3}] Erro ao carregar histórico: {str(e)}")
                countries_skipped.append(iso3)
                continue
            
            # Normalizar TRADE_TYPE antes de concatenar
            # Histórico usa 0/1, dados novos podem usar 'I'/'E'
            if 'TRADE_TYPE' in df_country.columns:
                # Converter 'I'/'E' para 1/0 (padrão Eurostat: 1=import, 0=export)
                trade_type_map = {'I': 1, 'E': 0}
                df_country['TRADE_TYPE'] = df_country['TRADE_TYPE'].map(
                    lambda x: trade_type_map.get(x, x) if isinstance(x, str) else x
                )
                df_country['TRADE_TYPE'] = pd.to_numeric(df_country['TRADE_TYPE'], errors='coerce').astype('Int64')
            
            # Concatenar dados novos com existentes
            if not df_existing.empty:
                # Garantir que o histórico também tem TRADE_TYPE numérico
                if 'TRADE_TYPE' in df_existing.columns:
                    df_existing['TRADE_TYPE'] = pd.to_numeric(df_existing['TRADE_TYPE'], errors='coerce').astype('Int64')
                df_updated = pd.concat([df_existing, df_country], ignore_index=True)
            else:
                df_updated = df_country.copy()
            
            # Normalizar tipos antes de salvar
            if 'PERIOD' in df_updated.columns:
                df_updated['PERIOD'] = pd.to_numeric(df_updated['PERIOD'], errors='coerce').astype('Int64')
            if 'VALUE_EUR' in df_updated.columns:
                df_updated['VALUE_EUR'] = pd.to_numeric(df_updated['VALUE_EUR'], errors='coerce')
            if 'QUANTITY_KG' in df_updated.columns:
                df_updated['QUANTITY_KG'] = pd.to_numeric(df_updated['QUANTITY_KG'], errors='coerce')
            
            # Criar diretório se não existir
            os.makedirs(hist_dir, exist_ok=True)
            
            # garantir que não há duplicatas
            df_updated.drop_duplicates(subset=['PRODUCT_NC', 'REPORTER', 'PARTNER', 'PERIOD', 'TRADE_TYPE', 'QUANTITY_KG'], keep='last', inplace=True)
            
            # Salvar arquivo atualizado
            try:
                df_updated.to_parquet(hist_path, index=False, engine='pyarrow')
                self.logger.info(f"[{iso3}] Histórico atualizado: {len(df_updated)} registros salvos em {hist_path}")
                countries_updated.append(iso3)
            except Exception as e:
                self.logger.error(f"[{iso3}] Erro ao salvar histórico: {str(e)}")
                countries_skipped.append(iso3)
        
        # 7. Resumo final
        self.logger.info("=" * 50)
        self.logger.info("RESUMO DA ATUALIZAÇÃO DE HISTÓRICOS")
        self.logger.info("=" * 50)
        self.logger.info(f"Países atualizados ({len(countries_updated)}): {', '.join(countries_updated)}")
        if countries_skipped:
            self.logger.warning(f"Países ignorados ({len(countries_skipped)}): {', '.join(countries_skipped)}")
        self.logger.info("=" * 50)
    
    def _country_specific_treatment(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Tratamento específico do país. Pode ser sobrescrito por subclasses.
        
        Args:
            df: DataFrame após normalização de colunas
            
        Returns:
            DataFrame tratado
        """
        # ajuste das datas
        df['Data'] = pd.to_datetime(df['Data'], format='%Y%m')
        return df


def main():
    """Execução local do pipeline completo."""
    # Criar instância do pipeline
    pipeline = COMEX_EUR(start_date=None, use_azure=False)
    
    # Executar pipeline completo
    # result_df = pipeline.run()
    pipeline.collect()
    
    # return result_df

def historic():
    # Criar instância do pipeline
    pipeline = COMEX_EUR(start_date=None, use_azure=False, last_months=60)
    
    pipeline.collect()
    pipeline.normalize()
    raw_import_df = pipeline.raw_import_df.copy()
    raw_export_df = pipeline.raw_export_df.copy()
    
    print(f"\nTotal registros importação histórica: {len(raw_import_df)}")
    print(f"\nTotal registros exportação histórica: {len(raw_export_df)}")
    pass

if __name__ == "__main__":
    result = main()
    print(f"\nPipeline concluído! Total de registros: {len(result)}")