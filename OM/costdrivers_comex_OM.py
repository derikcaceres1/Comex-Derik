from io import BytesIO
import yaml
import pandas as pd
import numpy as np
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from typing import Tuple, Optional, Dict, List
from pathlib import Path
import logging
import os
import sys

# Adicionar path da library ao sys.path para imports
project_root = Path(__file__).parent.parent
library_path = project_root / "library"
if str(library_path) not in sys.path:
    sys.path.insert(0, str(library_path))

from library.costdrivers import ApiAsync

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')


class ComexPipeline(ABC):
    """
    Classe base abstrata para pipelines de COMEX.
    Define as 4 fases do processamento: Coleta, Tratamento, Cálculo e Upload.
    """
    
    def __init__(self, iso_code: str, start_date: Optional[datetime] = None, 
                 data_contract_path: str = None, ids_table_path: str = None,
                 storage_base_path: str = "staging/comex",
                 use_azure: bool = True):
        """
        Inicializa o pipeline de COMEX.
        
        Args:
            iso_code: Código ISO do país (ex: 'ARG', 'BRA', 'CHL')
            start_date: Data inicial para coleta (padrão: 60 dias atrás)
            data_contract_path: Caminho para o arquivo data-contract.yaml
            ids_table_path: Caminho para a tabela de IDs
            storage_base_path: Caminho base no ADLS2
            use_azure: Se True, usa ADLS2; se False, usa storage local
        """
        self.iso_code = iso_code.upper()
        self.logger = logging.getLogger(f"{self.__class__.__name__}_{self.iso_code}")
        
        # Datas
        self.start_date = start_date if start_date else (datetime.now() - timedelta(days=60))
        self.start_date = self.start_date.replace(day=1)
        self.period_dates = self._generate_date_range(self.start_date)
        
        # Azure Storage
        self.use_azure = use_azure
        self.azure_storage = None
        if self.use_azure:
            try:
                from library.BlobStorage_API import AzureBlobStorage  # Importe sua classe
                self.azure_storage = AzureBlobStorage()
                self.logger.info("Conectado ao Azure Storage com sucesso")
            except Exception as e:
                self.logger.warning(f"Não foi possível conectar ao Azure Storage: {str(e)}")
                self.logger.warning("Usando storage local como fallback")
                self.use_azure = False
        
        # Paths
        # Se não estiver usando Azure, usar OM/dados/{iso_code} como padrão local
        if not self.use_azure and storage_base_path == "staging/comex":
            storage_base_path = "OM/dados"
        self.storage_base_path = storage_base_path
        self.raw_path = f"{storage_base_path}/{self.iso_code}/raw"
        self.silver_path = f"{storage_base_path}/{self.iso_code}/silver"
        self.gold_path = f"{storage_base_path}/{self.iso_code}/gold"
        
        # Configurações
        # Por padrão, usa data-contract.yaml na raiz (contém todos os países)
        self.data_contract_path = data_contract_path or "data-contract.yaml"
        self.ids_table_path = ids_table_path or "IDS_comex.xlsx"
        
        # DataFrames de trabalho
        self.raw_import_df = None
        self.raw_export_df = None
        self.silver_df = None
        self.gold_df = None
        
        self.logger.info(f"Pipeline inicializado para {self.iso_code}")
        self.logger.info(f"Período: {self.start_date.strftime('%Y-%m-%d')} até {datetime.now().strftime('%Y-%m-%d')}")
        self.logger.info(f"Storage: {'Azure ADLS2' if self.use_azure else 'Local'}")
    
    def _generate_date_range(self, start_date: datetime) -> List[datetime]:
        """Gera lista de datas mensais desde start_date até hoje."""
        dates = []
        current_date = start_date
        while current_date <= datetime.now():
            dates.append(current_date)
            if current_date.month == 12:
                current_date = current_date.replace(year=current_date.year + 1, month=1)
            else:
                current_date = current_date.replace(month=current_date.month + 1)
        return dates
    
    # ============= CACHE LOCAL (APENAS DESENVOLVIMENTO) =============
    
    def _is_local_environment(self) -> bool:
        """
        Verifica se está rodando em ambiente local (não Airflow).
        
        Returns:
            True se ambiente local, False se Airflow
        """
        try:
            # Verifica o caminho absoluto do arquivo atual
            current_file = Path(__file__).resolve()
            current_path_str = str(current_file)
            
            # Usar variável de ambiente se disponível
            airflow_include_path = os.getenv('AIRFLOW_INCLUDE_PATH', '/opt/airflow/include')
            if airflow_include_path in current_path_str:
                return False
            
            # Verifica também variáveis de ambiente comuns do Airflow
            if os.environ.get('AIRFLOW_HOME') or os.environ.get('AIRFLOW_CONFIG'):
                return False
            
            return True
        except Exception:
            # Em caso de erro, assume ambiente local por segurança
            return True
    
    def _get_cache_path(self) -> Path:
        """Retorna o caminho do diretório de cache."""
        cache_dir = Path(".cache") / "comex" / self.iso_code
        cache_dir.mkdir(parents=True, exist_ok=True)
        return cache_dir
    
    def _get_cache_file_path(self, cache_type: str) -> Path:
        """
        Retorna o caminho completo do arquivo de cache.
        
        Args:
            cache_type: 'import' ou 'export'
        """
        cache_dir = self._get_cache_path()
        # Nome do arquivo inclui o tipo e a data inicial para diferenciar caches
        cache_filename = f"{cache_type}_cache_{self.start_date.strftime('%Y%m%d')}.parquet"
        return cache_dir / cache_filename
    
    def _is_cache_valid(self, cache_file: Path) -> bool:
        """
        Verifica se o cache é válido (menos de 1 dia).
        
        Args:
            cache_file: Caminho do arquivo de cache
            
        Returns:
            True se cache válido, False caso contrário
        """
        if not cache_file.exists():
            return False
        
        try:
            # Verificar data de modificação do arquivo
            file_mtime = datetime.fromtimestamp(cache_file.stat().st_mtime)
            age = datetime.now() - file_mtime
            
            # Cache válido por 1 dia (24 horas)
            is_valid = age < timedelta(days=1)
            
            if is_valid:
                self.logger.info(f"Cache válido encontrado: {cache_file.name} (idade: {age})")
            else:
                self.logger.info(f"Cache expirado: {cache_file.name} (idade: {age})")
            
            return is_valid
        except Exception as e:
            self.logger.warning(f"Erro ao verificar cache: {str(e)}")
            return False
    
    def _load_from_cache(self, cache_type: str) -> Optional[pd.DataFrame]:
        """
        Carrega DataFrame do cache local.
        
        Args:
            cache_type: 'import' ou 'export'
            
        Returns:
            DataFrame se cache válido encontrado, None caso contrário
        """
        if not self._is_local_environment():
            return None
        
        cache_file = self._get_cache_file_path(cache_type)
        
        if not self._is_cache_valid(cache_file):
            return None
        
        try:
            df = pd.read_parquet(cache_file)
            self.logger.info(f"✓ Cache carregado: {cache_type} ({len(df)} registros)")
            return df
        except Exception as e:
            self.logger.warning(f"Erro ao carregar cache: {str(e)}")
            return None
    
    def _save_to_cache(self, df: pd.DataFrame, cache_type: str):
        """
        Salva DataFrame no cache local.
        
        Args:
            df: DataFrame para salvar
            cache_type: 'import' ou 'export'
        """
        if not self._is_local_environment():
            return
        
        if df.empty:
            self.logger.warning(f"DataFrame vazio, não salvando cache para {cache_type}")
            return
        
        try:
            cache_file = self._get_cache_file_path(cache_type)
            df.to_parquet(cache_file, index=False, compression='gzip')
            self.logger.info(f"✓ Cache salvo: {cache_type} ({len(df)} registros) em {cache_file}")
        except Exception as e:
            self.logger.warning(f"Erro ao salvar cache: {str(e)}")
    
    # ============= FASE 1: COLETA =============
    
    @abstractmethod
    def collect_import_data(self) -> pd.DataFrame:
        """
        Método abstrato para coletar dados de importação.
        Deve ser implementado por cada país específico.
        
        Returns:
            DataFrame com dados brutos de importação
        """
        pass
    
    @abstractmethod
    def collect_export_data(self) -> pd.DataFrame:
        """
        Método abstrato para coletar dados de exportação.
        Deve ser implementado por cada país específico.
        
        Returns:
            DataFrame com dados brutos de exportação
        """
        pass
    
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
        
        self.logger.info("=== FASE 1 CONCLUÍDA ===\n")
        return self.raw_import_df, self.raw_export_df
    
    # ============= FASE 2: TRATAMENTO =============
    
    def load_data_contract(self) -> Dict:
        """Carrega o arquivo data-contract.yaml com mapeamento de colunas."""
        # Tentar carregar do caminho especificado
        contract_path = Path(self.data_contract_path)
        
        # Tentar múltiplos caminhos possíveis (em ordem de prioridade)
        possible_paths = []
        
        # 1. Diretório serie_temporal (preferido - onde o arquivo está na NM)
        serie_temporal_path = Path(f"NM/serie_temporal/data-contract.yaml")
        possible_paths.append(("diretório serie_temporal", serie_temporal_path))
        
        # 2. Path especificado pelo usuário (relativo ou absoluto)
        if contract_path.is_absolute() or not str(contract_path).startswith('.'):
            possible_paths.append(("path especificado", contract_path))
        else:
            # Se relativo, tentar a partir da raiz do projeto
            possible_paths.append(("path especificado (raiz)", contract_path))
        
        # 3. Path padrão na raiz do projeto
        root_path = Path("data-contract.yaml")
        possible_paths.append(("raiz do projeto", root_path))
        
        # 4. Paths em config (se existir)
        possible_paths.append(("config específico", Path("config") / f"data_contract_{self.iso_code}.yaml"))
        possible_paths.append(("config padrão", Path("config") / "data-contract.yaml"))
        
        # Tentar cada path até encontrar o arquivo
        for path_name, path in possible_paths:
            if path.exists():
                try:
                    with open(path, 'r', encoding='utf-8') as file:
                        contract = yaml.safe_load(file)
                        self.logger.info(f"✓ Data contract carregado de {path_name}: {path.absolute()}")
                        return contract
                except Exception as e:
                    self.logger.warning(f"Erro ao carregar data contract de {path_name} ({path}): {str(e)}")
                    continue
            else:
                self.logger.debug(f"  Data contract não encontrado em {path_name}: {path.absolute()}")
        
        self.logger.error(f"Data contract não encontrado em nenhum dos caminhos tentados")
        self.logger.warning("Continuando sem data contract - normalização de colunas será ignorada")
        return {}
    
    def load_ids_table(self) -> pd.DataFrame:
        """Carrega a tabela de IDs filtrada pelo país."""
        try:
            # Tentar múltiplos caminhos possíveis (em ordem de prioridade)
            possible_paths = []
            
            # 1. Diretório library (preferido - onde o arquivo realmente está)
            library_path = Path(f"library/IDS_comex.xlsx")
            possible_paths.append(("diretório library", library_path))
            
            # 2. Path especificado pelo usuário (relativo ou absoluto)
            user_path = Path(self.ids_table_path)
            if user_path.is_absolute():
                possible_paths.append(("path especificado (absoluto)", user_path))
            else:
                # Se relativo, tentar a partir da raiz do projeto
                possible_paths.append(("path especificado (raiz)", user_path))
            
            # 3. Paths alternativos comuns
            possible_paths.append(("diretório include", Path("include") / self.ids_table_path))
            possible_paths.append(("diretório config", Path("config") / self.ids_table_path))
            possible_paths.append(("diretório data", Path("data") / self.ids_table_path))
            
            # Tentar cada path até encontrar o arquivo
            df_ids = None
            used_path = None
            for path_name, path in possible_paths:
                if path.exists():
                    try:
                        df_ids = pd.read_excel(path)
                        used_path = path
                        self.logger.info(f"✓ Tabela de IDs carregada de {path_name}: {path.absolute()}")
                        break
                    except Exception as e:
                        self.logger.warning(f"Erro ao carregar IDs de {path_name} ({path}): {str(e)}")
                        continue
                else:
                    self.logger.debug(f"  Tabela de IDs não encontrada em {path_name}: {path.absolute()}")
            
            # Se nenhum path funcionou, levantar erro
            if df_ids is None:
                error_msg = f"Arquivo {self.ids_table_path} não encontrado em nenhum dos caminhos tentados"
                self.logger.error(error_msg)
                raise FileNotFoundError(error_msg)
            
            # Filtrar por país
            country = self._get_country_name()
            ids = df_ids[df_ids['Pais_1'] == country].copy()
            if country == 'Brasil':
                # Filtrar apenas IDs com nova metodologia (IDIndicePrincipal >= 382958)
                nova_metodologia = 382958
                ids = ids[ids['IDIndicePrincipal'] < nova_metodologia]
            self.logger.info(f"IDs carregados: {len(ids)} registros para {self._get_country_name()}")
            return ids
        except FileNotFoundError:
            raise
        except Exception as e:
            self.logger.error(f"Erro ao carregar IDs: {str(e)}")
            raise
    
    @abstractmethod
    def _get_country_name(self) -> str:
        """Retorna o nome do país usado na tabela de IDs."""
        pass
    
    def normalize_columns(self, df: pd.DataFrame, contract: Dict, import_export: int) -> pd.DataFrame:
        """
        Normaliza nomes de colunas conforme data contract.
        
        Args:
            df: DataFrame para normalizar
            contract: Dicionário com o data contract carregado do YAML
            import_export: 1 para importação, 0 para exportação
            
        Returns:
            DataFrame com colunas normalizadas
        """
        if not contract:
            self.logger.warning("Data contract vazio ou não encontrado")
            return df
        
        # Obter seção do país (ex: ARG)
        country_section = contract.get(self.iso_code)
        if not country_section:
            self.logger.warning(f"Seção {self.iso_code} não encontrada no data contract")
            return df
        
        # Determinar tipo: import ou export
        ie_type = 'import' if import_export == 1 else 'export'
        type_mapping = country_section.get(ie_type)
        
        if not type_mapping:
            self.logger.warning(f"Mapeamento para {ie_type} não encontrado no data contract")
            return df
        
        # Criar dicionário de mapeamento (inverter: valor -> chave)
        # No YAML: coluna_original: coluna_normalizada
        # No pandas rename: {coluna_original: coluna_normalizada}
        mapping = {}
        for key, value in type_mapping.items():
            if key != 'columns':  # Ignorar a chave 'columns' que é apenas metadata
                mapping[key] = value
        
        if not mapping:
            self.logger.warning(f"Nenhum mapeamento encontrado para {ie_type}")
            return df
        
        # Aplicar renomeação
        df_normalized = df.rename(columns=mapping)
        self.logger.info(f"Colunas normalizadas ({ie_type}): {list(mapping.keys())} -> {list(mapping.values())}")
        
        # Garantir que a coluna Data está com o nome correto (maiúscula)
        if 'data' in df_normalized.columns and 'Data' not in df_normalized.columns:
            df_normalized.rename(columns={'data': 'Data'}, inplace=True)
        
        # Filtrar apenas as colunas especificadas no data contract
        expected_columns = type_mapping.get('columns', [])
        if expected_columns:
            # Manter apenas as colunas esperadas que existem no DataFrame
            available_columns = [col for col in expected_columns if col in df_normalized.columns]
            if available_columns:
                df_normalized = df_normalized[available_columns]
                self.logger.info(f"Colunas mantidas após normalização: {available_columns}")
            else:
                self.logger.warning(f"Nenhuma das colunas esperadas encontrada: {expected_columns}")
        
        return df_normalized
    
    def treat(self) -> pd.DataFrame:
        """
        Executa a fase de tratamento completa.
        
        Returns:
            DataFrame silver (tratado e filtrado)
        """
        self.logger.info("=== INICIANDO FASE 2: TRATAMENTO ===")
        
        # Verificar se temos dados brutos
        if self.raw_import_df is None or self.raw_export_df is None:
            self.logger.info("Carregando dados brutos do storage...")
            self.raw_import_df = self._load_from_storage(self.raw_path, "import_raw.parquet")
            self.raw_export_df = self._load_from_storage(self.raw_path, "export_raw.parquet")
        
        # Carregar data contract
        contract = self.load_data_contract()
        
        # Normalizar colunas separadamente para import e export
        # Importação (ImportExport = 1)
        self.logger.info("Normalizando colunas de IMPORTAÇÃO...")
        self.raw_import_df = self.normalize_columns(self.raw_import_df, contract, import_export=1)
        self.raw_import_df['ImportExport'] = 1
        
        # Exportação (ImportExport = 0)
        self.logger.info("Normalizando colunas de EXPORTAÇÃO...")
        self.raw_export_df = self.normalize_columns(self.raw_export_df, contract, import_export=0)
        self.raw_export_df['ImportExport'] = 0
        
        # Tratamento específico do país antes de concatenar
        self.raw_import_df = self._country_specific_treatment(self.raw_import_df)
        self.raw_export_df = self._country_specific_treatment(self.raw_export_df)
        
        # Padronizar tipos de dados usando schema do YAML
        self.raw_import_df = self._standardize_data_types(self.raw_import_df, contract)
        self.raw_export_df = self._standardize_data_types(self.raw_export_df, contract)
        
        # Concatenar
        df = pd.concat([self.raw_import_df, self.raw_export_df], ignore_index=True)
        self.logger.info(f"Dados concatenados: {len(df)} registros totais")
        
        # Carregar IDs e fazer merge
        ids = self.load_ids_table()
        ids['NCM'] = ids['NCM'].astype(str)
        ids['NCM'] = pd.to_numeric(ids['NCM'], errors='coerce').astype('Int64')
        
        # Garantir que a coluna ncm existe no DataFrame (pode estar em minúsculas)
        if 'ncm' in df.columns:
            df['NCM'] = df['ncm']
            df['NCM'] = pd.to_numeric(df['NCM'], errors='coerce').astype('Int64')
            df.dropna(subset=['NCM'], inplace=True)
        elif 'NCM' not in df.columns:
            self.logger.error("Coluna NCM/ncm não encontrada no DataFrame após normalização")
            raise ValueError("Coluna NCM/ncm não encontrada no DataFrame")
        
        self.logger.info("Realizando merge com tabela de IDs...")
        df_merged = df.merge(
            ids[['NCM', 'ImportExport', 'IDIndicePrincipal']], 
            on=['NCM', 'ImportExport'],
            how='inner'
        )
        self.logger.info(f"Após merge: {len(df_merged)} registros")
        
        # Preparar DataFrame silver
        # Manter ImportExport pois será necessário para cálculos posteriores
        df_merged.drop(columns=['NCM'], inplace=True)
        # Garantir que Data está com o nome correto (pode estar em minúsculas)
        if 'Data' not in df_merged.columns and 'data' in df_merged.columns:
            df_merged.rename(columns={'data': 'Data'}, inplace=True)
        df_merged.rename(columns={'IDIndicePrincipal': 'ID'}, inplace=True)
        
        # Filtrar e agregar
        df_merged = df_merged[df_merged['peso'] > 0]
        df_merged = df_merged.groupby(['ID', 'Data', 'ImportExport'], as_index=False).sum()
        
        # Calcular FOB
        df_merged['FOB'] = df_merged['valor'] / df_merged['peso']

        # checar se tem colunas para calcular CIF
        if ('frete' in df_merged.columns) and ('seguro' in df_merged.columns):
            df_merged['CIF'] = (df_merged['valor'] + df_merged['frete'] + df_merged['seguro']) / df_merged['peso']
        else:
            df_merged['CIF'] = np.nan
            
        self.silver_df = df_merged[['ID', 'Data', 'FOB', 'CIF', 'ImportExport']].copy()
        self.silver_df.drop_duplicates(subset=['ID', 'Data', 'ImportExport'], inplace=True)
        
        self.logger.info(f"Silver gerado: {len(self.silver_df)} registros")
        date_str = datetime.now().strftime('%Y-%m-%d')
        self._save_to_storage(self.silver_df, self.silver_path, f"silver_{date_str}.parquet")
        
        self.logger.info("=== FASE 2 CONCLUÍDA ===\n")
        return self.silver_df
    
    def _country_specific_treatment(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Tratamento específico do país. Pode ser sobrescrito por subclasses.
        
        Args:
            df: DataFrame após normalização de colunas
            
        Returns:
            DataFrame tratado
        """
        return df
    
    def _standardize_data_types(self, df: pd.DataFrame, contract: Dict) -> pd.DataFrame:
        """
        Padroniza tipos de dados das colunas conforme schema do data contract.
        
        Args:
            df: DataFrame para padronizar
            contract: Dicionário com o data contract carregado do YAML
            
        Returns:
            DataFrame com tipos de dados padronizados
        """
        if not contract or 'schema' not in contract:
            self.logger.warning("Schema não encontrado no data contract, usando padrão")
            return self._standardize_data_types_fallback(df)
        
        schema = contract['schema']
        
        # Mapear tipos do schema para funções de conversão
        type_mapping = {
            'datetime': lambda x: pd.to_datetime(x, errors='coerce'),
            'int': lambda x: pd.to_numeric(x, errors='coerce').astype('Int64'),
            'float': lambda x: pd.to_numeric(x, errors='coerce'),
            'str': lambda x: x.astype(str)
        }
        
        # Aplicar conversões baseadas no schema
        for column, dtype_str in schema.items():
            if column not in df.columns:
                continue
            
            # Converter string do tipo para função de conversão
            dtype_str_lower = dtype_str.lower()
            
            if dtype_str_lower == 'datetime':
                df[column] = pd.to_datetime(df[column], errors='coerce')

            elif dtype_str_lower == 'int':
                # Converter para string primeiro para tratar vírgulas
                if df[column].dtype == 'object':
                    df[column] = df[column].astype(str).str.replace(',', '.')
                df[column] = pd.to_numeric(df[column], errors='coerce')
                df.dropna(subset=[column], inplace=True)

            elif dtype_str_lower == 'float':
                # Converter para string primeiro para tratar vírgulas
                if df[column].dtype == 'object':
                    df[column] = df[column].astype(str).str.replace(',', '.')
                df[column] = pd.to_numeric(df[column], errors='coerce')

            elif dtype_str_lower == 'str':
                df[column] = df[column].astype(str)
            
            self.logger.debug(f"Coluna {column} convertida para {dtype_str}")
        
        # Tratamento especial para ImportExport (não está no schema mas é necessário)
        if 'ImportExport' in df.columns:
            df['ImportExport'] = df['ImportExport'].astype(int)
        
        return df
    
    def _standardize_data_types_fallback(self, df: pd.DataFrame) -> pd.DataFrame:
        """Fallback para padronização de tipos quando schema não está disponível."""
        # Peso
        if 'peso' in df.columns:
            df['peso'] = df['peso'].astype(str).str.replace(',', '.')
            df['peso'] = pd.to_numeric(df['peso'], errors='coerce')
        
        # Valor
        if 'valor' in df.columns:
            df['valor'] = df['valor'].astype(str).str.replace(',', '.')
            df['valor'] = pd.to_numeric(df['valor'], errors='coerce')
        
        # NCM
        if 'ncm' in df.columns:
            df['ncm'] = pd.to_numeric(df['ncm'], errors='coerce')
            df.dropna(subset=['ncm'], inplace=True)
            df['ncm'] = df['ncm'].astype(int)
        
        # ImportExport
        if 'ImportExport' in df.columns:
            df['ImportExport'] = df['ImportExport'].astype(int)
        
        # Data
        if 'Data' in df.columns:
            df['Data'] = pd.to_datetime(df['Data'])
        
        return df
    
    # ============= FASE 3: CÁLCULO =============
    
    @staticmethod
    def outlier_testing_iqr(series: pd.DataFrame, date_column: str = 'Data', value_column: str = 'Valor') -> pd.DataFrame:
        """
        Detecta e corrige outliers usando IQR cumulativo.
        Para cada ponto, calcula Q1, Q3 e IQR baseado apenas nos dados históricos até aquele ponto.
        Outliers são substituídos por NaN e depois interpolados.
        
        Args:
            series: DataFrame com colunas 'ID', date_column e value_column
            date_column: Nome da coluna de data (padrão: 'Data')
            value_column: Nome da coluna de valor (padrão: 'Valor')
            
        Returns:
            DataFrame com outliers corrigidos e interpolados
        """
        # Criar cópia e garantir que ID está presente
        series_clean = series.copy()
        
        # Obter ID antes de definir índice
        if 'ID' in series_clean.columns:
            id_value = series_clean['ID'].iloc[0] if len(series_clean) > 0 else None
        else:
            id_value = None
        
        # Definir índice como data
        series_clean = series_clean.set_index(date_column).copy()
        
        # Obter valores brutos
        raw_values = series_clean.get(value_column)
        if raw_values is None:
            raise ValueError(f"'{value_column}' column is required for raw outlier detection.")
        
        raw_values_np = raw_values.values
        
        # Calcular Q1, Q3 e IQR cumulativos para cada ponto
        q1_list, q3_list, iqr_list = [], [], []
        
        for i in range(len(raw_values_np)):
            # Usar apenas dados históricos até o ponto atual
            past_raw = raw_values_np[:i+1]
            past_raw = past_raw[~np.isnan(past_raw)]
            # 
            if len(past_raw) >= 1:
                q1 = np.percentile(past_raw, 25)
                q3 = np.percentile(past_raw, 75)
                iqr = q3 - q1
            else:
                q1 = q3 = iqr = np.nan
            # 
            q1_list.append(q1)
            q3_list.append(q3)
            iqr_list.append(iqr)
        
        series_clean['cumulative_Q1'] = q1_list
        series_clean['cumulative_Q3'] = q3_list
        series_clean['cumulative_IQR'] = iqr_list
        
        # Detectar outliers usando limites IQR
        lower_bound = series_clean['cumulative_Q1'] - 2 * series_clean['cumulative_IQR']
        upper_bound = series_clean['cumulative_Q3'] + 2 * series_clean['cumulative_IQR']
        
        iqr_outliers = (raw_values < lower_bound) | (raw_values > upper_bound)
        
        # Substituir outliers por NaN
        series_clean.loc[iqr_outliers, value_column] = np.nan

        # criar coluna de suavisado
        series_clean.loc[iqr_outliers, 'suavisado'] = True
        series_clean.loc[~iqr_outliers, 'suavisado'] = False
        
        # Interpolar valores faltantes usando método linear
        series_clean[value_column] = series_clean[value_column].interpolate(method="linear")
        
        # Resetar índice e adicionar ID
        series_clean.reset_index(inplace=True)
        series_clean['ID'] = id_value
        
        # Retornar apenas colunas necessárias
        series_clean = series_clean[['ID', date_column, value_column, 'suavisado']]
        
        return series_clean
    
    def calculate(self) -> pd.DataFrame:
        """
        Executa a fase de cálculo completa.
        
        Returns:
            DataFrame gold (com cálculos finais)
        """
        self.logger.info("=== INICIANDO FASE 3: CÁLCULO ===")
        
        # Verificar se temos dados silver
        if self.silver_df is None:
            self.logger.info("Carregando dados silver do storage...")
            self.silver_df = self._load_from_storage(self.silver_path, "silver.parquet")
        
        # Verificar se CIF está disponível
        has_cif = 'CIF' in self.silver_df.columns
        
        # Preparar DataFrame para outlier_testing_iqr
        # O método espera colunas ID, Data e Valor
        # No silver_df temos ID, Data, FOB, CIF, ImportExport
        df_for_outlier = self.silver_df.copy()
        
        # Renomear FOB para Valor temporariamente para outlier_testing_iqr
        df_for_outlier['Valor'] = df_for_outlier['FOB']
        
        # Aplicar outlier_testing_iqr agrupando por ID
        self.logger.info("Aplicando detecção e correção de outliers (IQR cumulativo)...")
        df_suavizado = df_for_outlier.groupby('ID').apply(
            lambda x: self.outlier_testing_iqr(x, date_column='Data', value_column='Valor')
        )
        
        # Resetar índice (groupby.apply pode criar MultiIndex)
        if isinstance(df_suavizado.index, pd.MultiIndex):
            # Se MultiIndex, remover nível de grupo e manter apenas dados
            df_suavizado = df_suavizado.reset_index(level=0, drop=True).reset_index(drop=True)
        else:
            df_suavizado = df_suavizado.reset_index(drop=True)
        
        # Renomear Valor de volta para FOB e reconstruir silver_df com valores suavizados
        df_suavizado.rename(columns={'Valor': 'FOB'}, inplace=True)
        
        # Manter outras colunas do silver_df original (CIF, ImportExport)
        df_suavizado = df_suavizado.merge(
            self.silver_df[['ID', 'Data', 'CIF', 'ImportExport']],
            on=['ID', 'Data'],
            how='left'
        )
        
        self.logger.info(f"Dados suavizados: {len(df_suavizado)} registros")
        
        # Calcular variação usando dados suavizados
        self.logger.info("Calculando variações...")
        df_resultado = calculate_variation(df_suavizado)
        
        # Obter cost drivers (usar silver_df original para obter IDs e datas)
        self.logger.info("Obtendo dados de cost drivers...")
        dataframe_costdriver = get_costdrivers_data(self.silver_df)
        
        df = calculate_new_values(df_resultado, dataframe_costdriver)
        df['suavisado'] = df['suavisado'].fillna(False)

        # Preencher lacunas
        self.logger.info("Preenchendo lacunas...")
        df_preenchido = preenche_lacuna(df)
        suavisado = df_preenchido.merge(df, on=['ID', 'Data'], how='left')['suavisado']

        df_preenchido.reset_index(drop=True, inplace=True)
        df_preenchido['suavisado'] = suavisado
        df_preenchido.loc[df_preenchido['suavisado'].isna(), 'suavisado'] = 'interpolado'
        df_preenchido.loc[df_preenchido['suavisado'] == True, 'suavisado'] = 'suavisado'
        df_preenchido.loc[df_preenchido['suavisado'] == False, 'suavisado'] = ''
        df_preenchido['suavisado'] = df_preenchido['suavisado'].astype(str)
        
        # Filtrar IDs com apenas 1 registro (verificar se ID existe primeiro)
        if not df_preenchido.empty and 'ID' in df_preenchido.columns:
            df_preenchido = df_preenchido[
                df_preenchido.groupby('ID')['Data'].transform('count') > 1
            ]
        elif not df_preenchido.empty:
            self.logger.warning("DataFrame preenchido não contém coluna 'ID', pulando filtro")
        
                
        def guardrail_interpolate_time_series(
                df: pd.DataFrame,
                id_col: str = 'ID',
                date_col: str = 'Data',
                value_cols: list = None,
                freq: str = 'MS',
                return_quality_report: bool = False,
            ):
            """
            Guardrail de séries temporais por ID com interpolação linear.

            Passos:
            1. Garante continuidade temporal por ID
            2. Interpola linearmente apenas lacunas internas
            3. Não extrapola início/fim
            4. Valida ausência de buracos internos
            """

            if value_cols is None:
                value_cols = ['Valor']

            df = df.copy()

            # -----------------------------
            # Normalização de data
            # -----------------------------
            df[date_col] = pd.to_datetime(df[date_col])

            dfs = []
            quality = []

            for id_, group in df.groupby(id_col):
                group = group.sort_values(date_col)

                # -----------------------------
                # Range completo de datas
                # -----------------------------
                full_range = pd.date_range(
                    start=group[date_col].min(),
                    end=group[date_col].max(),
                    freq=freq
                )

                group_full = (
                    group
                    .set_index(date_col)
                    .reindex(full_range)
                    .rename_axis(date_col)
                    .reset_index()
                )

                group_full[id_col] = id_

                # -----------------------------
                # Interpolação linear segura
                # -----------------------------
                for col in value_cols:
                    group_full[col] = group_full[col].interpolate(
                        method='linear',
                        limit_area='inside',
                        limit=3
                    )

                # -----------------------------
                # Validação pós-interpolação
                # NaN internos são esperados quando gap > limit=3 meses
                # -----------------------------
                for col in value_cols:
                    first = group_full[col].first_valid_index()
                    last = group_full[col].last_valid_index()

                    if first is not None and last is not None:
                        internal = group_full.loc[first:last, col]
                        if internal.isna().any():
                            nan_count = internal.isna().sum()
                            logging.getLogger(__name__).debug(
                                f"ID={id_}, coluna={col}: {nan_count} mês(es) "
                                f"sem dado após interpolação (gap > 3 meses)"
                            )

                dfs.append(group_full)

            if not dfs:
                return pd.DataFrame()

            result = pd.concat(dfs, ignore_index=True)

            if return_quality_report:
                return result, pd.DataFrame(quality)

            return result
        
        # gardrail: final interpolation per ID
        final = guardrail_interpolate_time_series(df_preenchido, value_cols=['Valor', 'Valor_Cif'])
        
        self.gold_df = final
        self.logger.info(f"Gold gerado: {len(self.gold_df)} registros")
        date_str = datetime.now().strftime('%Y-%m-%d')
        self._save_to_storage(self.gold_df, self.gold_path, f"gold_{date_str}.parquet")
        
        self.logger.info("=== FASE 3 CONCLUÍDA ===\n")
        return self.gold_df
    
    # ============= FASE 4: UPLOAD =============
    
    def upload(self) -> bool:
        """
        Executa a fase de upload completa.
        
        Returns:
            True se upload bem-sucedido
        """
        self.logger.info("=== INICIANDO FASE 4: UPLOAD ===")
        
        # Verificar se temos dados gold
        if self.gold_df is None:
            self.logger.info("Carregando dados gold do storage...")
            self.gold_df = self._load_from_storage(self.gold_path, "gold.parquet")
        
        # Realizar upload
        self.logger.info(f"Fazendo upload de {len(self.gold_df)} registros...")
        try:
            upload_data(self.gold_df)
            self.logger.info("Upload concluído com sucesso!")
            self.logger.info("=== FASE 4 CONCLUÍDA ===\n")
            return True
        except Exception as e:
            self.logger.error(f"Erro no upload: {str(e)}")
            raise
    
    # ============= PIPELINE COMPLETO =============
    
    def run(self, skip_phases: List[str] = None) -> pd.DataFrame:
        """
        Executa o pipeline completo de COMEX.
        
        Args:
            skip_phases: Lista de fases para pular ['collect', 'treat', 'calculate', 'upload']
        
        Returns:
            DataFrame final (gold)
        """
        skip_phases = skip_phases or []
        
        self.logger.info(f"\n{'='*60}")
        self.logger.info(f"INICIANDO PIPELINE COMEX - {self.iso_code}")
        self.logger.info(f"{'='*60}\n")
        
        try:
            if 'collect' not in skip_phases:
                self.collect()
            
            if 'treat' not in skip_phases:
                self.treat()
            
            if 'calculate' not in skip_phases:
                self.calculate()
            
            if 'upload' not in skip_phases:
                # self.upload()
                pass
            
            self.logger.info(f"\n{'='*60}")
            self.logger.info(f"PIPELINE CONCLUÍDO COM SUCESSO - {self.iso_code}")
            self.logger.info(f"{'='*60}\n")
            
            return self.gold_df
            
        except Exception as e:
            self.logger.error(f"\n{'='*60}")
            self.logger.error(f"ERRO NO PIPELINE - {self.iso_code}")
            self.logger.error(f"Erro: {str(e)}")
            self.logger.error(f"{'='*60}\n")
            raise
    
    # ============= MÉTODOS DE STORAGE =============
    
    def _save_to_storage(self, df: pd.DataFrame, path: str, filename: str):
        """
        Salva DataFrame no storage (ADLS2 ou local).
        
        Args:
            df: DataFrame para salvar
            path: Caminho do diretório (ex: 'staging/comex/ARG/raw')
            filename: Nome do arquivo (ex: 'import_raw.parquet')
        """
        full_path_str = f"{path}/{filename}"
        
        if self.use_azure and self.azure_storage:
            try:
                # Usar Azure Storage
                self.logger.info(f"Salvando no ADLS2: {full_path_str}")
                
                # A classe AzureBlobStorage já adiciona 'staging/rpa-tracking/' no método add_file
                # Então precisamos remover 'staging/' do nosso path se existir
                azure_path = full_path_str
                if azure_path.startswith('staging/'):
                    azure_path = azure_path.replace('staging/', '', 1)
                
                # Preparar buffer com o parquet
                buffer = BytesIO()
                df.to_parquet(
                    path=buffer,
                    engine='pyarrow',
                    compression='gzip',
                    index=False
                )
                buffer = buffer.getvalue()
                
                # Upload para Azure
                self.azure_storage.add_file(buffer=buffer, blob_name=azure_path)
                self.logger.info(f"✓ Dados salvos no ADLS2: {azure_path}")
                return
                
            except Exception as e:
                self.logger.error(f"Erro ao salvar no Azure: {str(e)}")
                self.logger.warning("Tentando salvar localmente como fallback...")
        
        # Fallback: salvar localmente
        from pathlib import Path
        full_path = Path(path)
        full_path.mkdir(parents=True, exist_ok=True)
        
        file_path = full_path / filename
        df.to_parquet(file_path, index=False, compression='gzip')
        self.logger.info(f"✓ Dados salvos localmente: {file_path}")
    
    def _load_from_storage(self, path: str, filename: str) -> pd.DataFrame:
        """
        Carrega DataFrame do storage (ADLS2 ou local).
        Se o filename não contiver data, procura pelo arquivo mais recente com padrão {base}_{date}.parquet
        
        Args:
            path: Caminho do diretório (ex: 'staging/comex/ARG/raw')
            filename: Nome do arquivo (ex: 'import_raw.parquet' ou 'import_raw_2024-01-15.parquet')
            
        Returns:
            DataFrame carregado
        """
        import re
        from pathlib import Path
        
        # Verificar se filename já contém data no formato YYYY-MM-DD
        date_pattern = r'_\d{4}-\d{2}-\d{2}\.parquet$'
        has_date = re.search(date_pattern, filename)
        
        if not has_date:
            # Se não tem data, procurar pelo arquivo mais recente
            # Extrair base do nome (ex: 'import_raw' de 'import_raw.parquet')
            base_name = filename.replace('.parquet', '')
            pattern = f"{base_name}_*.parquet"
            
            if self.use_azure and self.azure_storage:
                # Para Azure, tentar apenas arquivo com data de hoje
                # Se não encontrar, significa que precisa de manutenção
                today_str = datetime.now().strftime('%Y-%m-%d')
                filename_with_date = f"{base_name}_{today_str}.parquet"
                full_path_str = f"{path}/{filename_with_date}"
                
                azure_path = full_path_str
                if azure_path.startswith('staging/'):
                    azure_path = azure_path.replace('staging/', '', 1)
                
                try:
                    file_bytes = self.azure_storage.download_adls2(azure_path)
                    df = pd.read_parquet(BytesIO(file_bytes))
                    self.logger.info(f"✓ Dados carregados do ADLS2: {azure_path} ({len(df)} registros)")
                    return df
                except Exception as e:
                    # Se não encontrar com data de hoje, logar erro e tentar fallback
                    self.logger.error(f"Arquivo com data de hoje não encontrado: {azure_path}")
                    self.logger.error(f"Erro: {str(e)}")
                    self.logger.warning(f"Tentando arquivo sem data para compatibilidade: {filename}...")
            else:
                # Para storage local, tentar apenas arquivo com data de hoje
                # Se não encontrar, significa que precisa de manutenção
                today_str = datetime.now().strftime('%Y-%m-%d')
                filename_with_date = f"{base_name}_{today_str}.parquet"
                file_path_today = Path(path) / filename_with_date
                
                if file_path_today.exists():
                    filename = filename_with_date
                    self.logger.info(f"Arquivo com data de hoje encontrado: {filename}")
                else:
                    # Se não encontrar com data de hoje, logar erro e tentar fallback
                    self.logger.error(f"Arquivo com data de hoje não encontrado: {file_path_today}")
                    self.logger.warning(f"Tentando arquivo sem data para compatibilidade: {filename}...")
        
        full_path_str = f"{path}/{filename}"
        
        if self.use_azure and self.azure_storage:
            try:
                # Usar Azure Storage
                self.logger.info(f"Carregando do ADLS2: {full_path_str}")
                
                # Ajustar path para Azure (remover 'staging/' se existir)
                azure_path = full_path_str
                if azure_path.startswith('staging/'):
                    azure_path = azure_path.replace('staging/', '', 1)
                
                # Download do Azure
                file_bytes = self.azure_storage.download_adls2(azure_path)
                
                # Converter bytes para DataFrame
                df = pd.read_parquet(BytesIO(file_bytes))
                self.logger.info(f"✓ Dados carregados do ADLS2: {azure_path} ({len(df)} registros)")
                return df
                
            except Exception as e:
                self.logger.error(f"Erro ao carregar do Azure: {str(e)}")
                self.logger.warning("Tentando carregar localmente como fallback...")
        
        # Fallback: carregar localmente
        file_path = Path(path) / filename
        
        if not file_path.exists():
            self.logger.error(f"Arquivo não encontrado: {file_path}")
            return pd.DataFrame()
        
        df = pd.read_parquet(file_path)
        self.logger.info(f"✓ Dados carregados localmente: {file_path} ({len(df)} registros)")
        return df


# ============= FUNÇÕES COMUNS =============
# Funções de cálculo e processamento de dados COMEX

def calcular_variacao_percentual(grupo: pd.DataFrame) -> pd.DataFrame:
    """
    Calcula a variação percentual mês a mês da coluna 'FOB' para um grupo de dados com base no campo 'Data'.
    Se CIF estiver disponível, também calcula alpha_FOB2CIF = CIF / FOB.
    Esta função é compatível com operações de `groupby`, permitindo o cálculo da variação para múltiplos
    grupos identificados por 'ID' ou outra chave.
    
    Args:
        grupo: DataFrame com as colunas 'Data' (datetime) e 'FOB' (FOB numérico)
               Opcionalmente pode conter 'CIF' para cálculo de alpha_FOB2CIF
        
    Returns:
        DataFrame com coluna adicional 'variacao_percentual_FOB' contendo a variação percentual mês a mês
    """
    if 'Data' not in grupo.columns or 'FOB' not in grupo.columns:
        raise ValueError("O DataFrame deve conter as colunas 'Data' e 'FOB'.")
    
    grupo = grupo.copy()
    # Garante que 'Data' esteja em formato datetime
    grupo['Data'] = pd.to_datetime(grupo['Data'], errors='coerce')
    
    grupo['variacao_percentual_FOB'] = grupo['FOB'].pct_change()
    grupo['alpha_FOB2CIF'] = grupo['CIF'] / grupo['FOB']
    return grupo


def preencher_lacunas_com_media(df):
    # Verificar se DataFrame está vazio
    if df.empty:
        return pd.DataFrame()
    
    # Verificar se colunas obrigatórias existem
    if 'ID' not in df.columns:
        raise ValueError("DataFrame deve conter coluna 'ID'")
    
    # Verificar se coluna Data existe (pode estar em minúsculas)
    if 'Data' not in df.columns:
        if 'data' in df.columns:
            df = df.rename(columns={'data': 'Data'})
        else:
            raise ValueError("DataFrame deve conter coluna 'Data' ou 'data'")
    
    if 'Valor' not in df.columns:
        raise ValueError("DataFrame deve conter coluna 'Valor'")
    
    # Verificar se ImportExport existe, caso contrário inferir a partir de Valor_Cif
    has_importexport = 'ImportExport' in df.columns
    has_valor_cif = 'Valor_Cif' in df.columns
    
    if has_importexport:
        ImportExport = df['ImportExport'].iloc[0]
    elif has_valor_cif:
        # Se Valor_Cif existe e não é todo NaN, assumir que é importação
        ImportExport = 1 if df['Valor_Cif'].notna().any() else 0
    else:
        # Se não há nem ImportExport nem Valor_Cif, assumir exportação (0)
        ImportExport = 0
    
    # Converte a coluna 'Data' para o tipo datetime, se ainda não for
    df['Data'] = pd.to_datetime(df['Data'])
    # Define 'Data' como índice para usar a função resample
    df_indexed = df.set_index('Data').sort_index()
    # Cria um DataFrame vazio para armazenar os resultados
    df_interp = list()
    # Processa cada ID individualmente
    for id_unico in df_indexed['ID'].unique():
        # Filtra os dados para o ID atual
        df_id = df_indexed[df_indexed['ID'] == id_unico].copy()
        
        # Remover coluna ID antes do resample (não é numérica)
        # Ela será adicionada de volta depois
        if 'ID' in df_id.columns:
            df_id = df_id.drop(columns=['ID'])
        
        # Resample para frequência mensal ('MS') para criar os meses ausentes
        df_resampled = df_id.resample('MS').mean()
        
        # Verificar se o resample retornou dados
        if df_resampled.empty:
            continue
        
        # Atribuir ID de volta (importante fazer antes de interpolar)
        df_resampled['ID'] = id_unico
        
        # Interpola os valores faltantes com interpolação linear (máx 3 meses consecutivos)
        if 'Valor' in df_resampled.columns:
            df_resampled['Valor'] = df_resampled['Valor'].interpolate(method='linear', limit=3)
        if ImportExport == 1 and 'Valor_Cif' in df_resampled.columns:
            df_resampled['Valor_Cif'] = df_resampled['Valor_Cif'].interpolate(method='linear', limit=3)
        
        # Verificar se ID está presente antes de adicionar
        if 'ID' in df_resampled.columns:
            # Adicionar o resultado ao DataFrame final
            df_interp.append(df_resampled)
    # Verificar se há dados para concatenar
    if not df_interp:
        return pd.DataFrame()
    
    # Reseta o índice para 'Data' ser uma coluna novamente
    df_interp = pd.concat(df_interp).reset_index()
    
    # Garantir que ID está presente no resultado final
    if 'ID' not in df_interp.columns:
        raise ValueError("Coluna 'ID' foi perdida durante o processamento")
    
    return df_interp


def calculate_variation(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calcula variações dos valores ao longo do tempo para cada ID.
    
    Args:
        df: DataFrame com colunas 'ID', 'Data', 'FOB', 'CIF' e 'ImportExport'
        
    Returns:
        DataFrame com coluna 'variacao_percentual' adicionada
        Se CIF disponível, também inclui 'variacao_percentual_FOB' e 'alpha_FOB2CIF'
    """
    # Manter ImportExport se disponível para agrupamento
    groupby_cols = ['ID']
    if 'ImportExport' in df.columns:
        groupby_cols.append('ImportExport')
    
    df_resultado = df.groupby(groupby_cols).apply(calcular_variacao_percentual).reset_index(drop=True)
    df_resultado.sort_values(by=['ID', 'Data'], inplace=True)
    
    # Remover apenas linhas onde variacao_percentual é NaN (não remover se tiver alpha_FOB2CIF)
    if 'variacao_percentual' in df_resultado.columns:
        df_resultado.dropna(subset=['variacao_percentual'], inplace=True)
    
    return df_resultado


def _is_local_environment_standalone() -> bool:
    """
    Verifica se está rodando em ambiente local (não Airflow).
    Função standalone para uso fora da classe.
    
    Returns:
        True se ambiente local, False se Airflow
    """
    try:
        # Verifica o caminho absoluto do arquivo atual
        current_file = Path(__file__).resolve()
        current_path_str = str(current_file)
        
        # Usar variável de ambiente se disponível
        airflow_include_path = os.getenv('AIRFLOW_INCLUDE_PATH', '/opt/airflow/include')
        if airflow_include_path in current_path_str:
            return False
        
        # Verifica também variáveis de ambiente comuns do Airflow
        if os.environ.get('AIRFLOW_HOME') or os.environ.get('AIRFLOW_CONFIG'):
            return False
        
        return True
    except Exception:
        # Em caso de erro, assume ambiente local por segurança
        return True


def _get_costdrivers_cache_path() -> Path:
    """Retorna o caminho do diretório de cache para cost drivers."""
    cache_dir = Path(".cache") / "comex" / "costdrivers"
    cache_dir.mkdir(parents=True, exist_ok=True)
    return cache_dir


def _get_costdrivers_cache_file_path(df: pd.DataFrame) -> Path:
    """
    Retorna o caminho completo do arquivo de cache baseado nos IDs e datas do DataFrame.
    
    Args:
        df: DataFrame com colunas 'ID' e 'Data'
    """
    cache_dir = _get_costdrivers_cache_path()
    # Criar hash baseado nos IDs e datas para identificar unicamente a requisição
    lista_ids = sorted(df['ID'].unique().tolist())
    data_min = df['Data'].min().strftime("%Y%m%d")
    data_max = df['Data'].max().strftime("%Y%m%d")
    ids_hash = hash(tuple(lista_ids)) % (10 ** 8)  # Hash dos IDs
    cache_filename = f"costdrivers_cache_{ids_hash}_{data_min}_{data_max}.parquet"
    return cache_dir / cache_filename


def _is_costdrivers_cache_valid(cache_file: Path) -> bool:
    """
    Verifica se o cache é válido (menos de 1 dia).
    
    Args:
        cache_file: Caminho do arquivo de cache
        
    Returns:
        True se cache válido, False caso contrário
    """
    if not cache_file.exists():
        return False
    
    try:
        logger = logging.getLogger(__name__)
        # Verificar data de modificação do arquivo
        file_mtime = datetime.fromtimestamp(cache_file.stat().st_mtime)
        age = datetime.now() - file_mtime
        
        # Cache válido por 1 dia (24 horas)
        is_valid = age < timedelta(days=1)
        
        if is_valid:
            logger.info(f"Cache válido encontrado: {cache_file.name} (idade: {age})")
        else:
            logger.info(f"Cache expirado: {cache_file.name} (idade: {age})")
        
        return is_valid
    except Exception as e:
        logger = logging.getLogger(__name__)
        logger.warning(f"Erro ao verificar cache: {str(e)}")
        return False


def _load_costdrivers_from_cache(df: pd.DataFrame) -> Optional[pd.DataFrame]:
    """
    Carrega DataFrame do cache local para cost drivers.
    
    Args:
        df: DataFrame com colunas 'ID' e 'Data' usado para identificar o cache
        
    Returns:
        DataFrame se cache válido encontrado, None caso contrário
    """
    if not _is_local_environment_standalone():
        return None
    
    cache_file = _get_costdrivers_cache_file_path(df)
    
    if not _is_costdrivers_cache_valid(cache_file):
        return None
    
    try:
        logger = logging.getLogger(__name__)
        df_cached = pd.read_parquet(cache_file)
        logger.info(f"✓ Cache carregado: costdrivers ({len(df_cached)} registros)")
        return df_cached
    except Exception as e:
        logger = logging.getLogger(__name__)
        logger.warning(f"Erro ao carregar cache: {str(e)}")
        return None


def _save_costdrivers_to_cache(df: pd.DataFrame, result_df: pd.DataFrame):
    """
    Salva DataFrame no cache local para cost drivers.
    
    Args:
        df: DataFrame original usado para identificar o cache
        result_df: DataFrame com dados de cost drivers para salvar
    """
    if not _is_local_environment_standalone():
        return
    
    if result_df.empty:
        return
    
    try:
        logger = logging.getLogger(__name__)
        cache_file = _get_costdrivers_cache_file_path(df)
        result_df.to_parquet(cache_file, index=False)
        logger.info(f"✓ Cache salvo: {cache_file.name}")
    except Exception as e:
        logger = logging.getLogger(__name__)
        logger.warning(f"Erro ao salvar cache: {str(e)}")


def get_costdrivers_data(df: pd.DataFrame, n_months: int = 3) -> pd.DataFrame:
    """
    Obtém dados de cost drivers da API.
    Usa cache local com expiração de 1 dia quando em ambiente local.
    
    Args:
        df: DataFrame com colunas 'ID' e 'Data'
        
    Returns:
        DataFrame com dados de cost drivers
    """
    logger = logging.getLogger(__name__)
    
    # Tentar carregar do cache primeiro
    cached_data = _load_costdrivers_from_cache(df)
    if cached_data is not None:
        return cached_data
    
    try:
        # Tentar importar dependências necessárias
        import sys
        import os
        from pathlib import Path
        
        # Tentar importar ApiAsync do módulo costdrivers
        try:
            # Adicionar pasta old ao path se necessário
            old_path = Path(__file__).parent / 'old'
            if str(old_path) not in sys.path:
                sys.path.insert(0, str(old_path))
                        
            endpoint_cost = 'https://api-costdrivers.gep.com/costdrivers-api'
            pass_word = 'i2024nb4'
            
            def make_lista_requisicao(lista_ids: list, data_min: str, data_max: str):
                """Cria lista de requisições para a API de cost drivers."""
                total_ids = len(lista_ids)
                lista_requisicao = []
                for i in range(0, total_ids, 49):
                    dict_ = {
                        'url': f'{endpoint_cost}/api/v1/DataScience/option/11',
                        'method': 'get'
                    }
                    if (total_ids - i) < 49:
                        dict_['params'] = {
                            'ids': ','.join(list(map(str, lista_ids[i:]))),
                            'dataCalculo1': data_min,
                            'dataCalculo2': data_max
                        }
                    else:
                        dict_['params'] = {
                            'ids': ','.join(list(map(str, lista_ids[i:i + 49]))),
                            'dataCalculo1': data_min,
                            'dataCalculo2': data_max
                        }
                    lista_requisicao.append(dict_)
                return lista_requisicao
            
            def get_token_costdrivers():
                """Obtém token de autenticação da API."""
                headers = {
                    'key': '070F0E8A-E0C6-4970-8865-480650C0D12C',
                    'email': 'datascience@datamark.com.br',
                    'pass': pass_word
                }
                req = {
                    'url': f'{endpoint_cost}/api/v1/Auth',
                    'method': 'GET'
                }
                apiasync = ApiAsync(True, [req], headers=headers)
                resp = apiasync.run()[0]['result']['tokenCode']
                return resp
            
            # Obter dados
            logger.info("Buscando dados de cost drivers da API...")
            lista_ids = df['ID'].unique().tolist()
            # Pegar 3 meses antes da menor data
            data_min_3meses = (df['Data'].min() - pd.DateOffset(months=n_months)).strftime("%d-%m-%Y")
            data_min = data_min_3meses
            data_max = df['Data'].max().strftime("%d-%m-%Y")
            
            lista_requisicao = make_lista_requisicao(lista_ids, data_min, data_max)
            headers = {'Authorization': 'Bearer ' + get_token_costdrivers()}
            dados_costdrivers = ApiAsync(True, lista_requisicao, headers=headers).run()
            
            lista_dataframe = [
                pd.DataFrame(rsp['result'])
                for rsp in dados_costdrivers
                if (not 'Error' in rsp.keys())
            ]
            
            if lista_dataframe:
                dataframe_costdriver = pd.concat(lista_dataframe, ignore_index=True)
                dataframe_costdriver = dataframe_costdriver.query("base == 0")
                dataframe_costdriver = dataframe_costdriver[['indicePrincipalID', 'dataIndice', 'valor']]
                dataframe_costdriver.columns = ['ID', 'Data', 'Valor']
                dataframe_costdriver['Data'] = pd.to_datetime(dataframe_costdriver['Data'], format='%Y-%m-%dT%H:%M:%S')
                
                # Salvar no cache
                _save_costdrivers_to_cache(df, dataframe_costdriver)
                
                return dataframe_costdriver
            else:
                return pd.DataFrame()
                
        except ImportError as e:
            logger.warning(f"Não foi possível importar ApiAsync: {str(e)}")
            logger.warning("Retornando DataFrame vazio - funcionalidade de cost drivers desabilitada")
            return pd.DataFrame()
        except Exception as e:
            logger.error(f"Erro ao obter dados de cost drivers: {str(e)}")
            return pd.DataFrame()
            
    except Exception as e:
        logger.error(f"Erro geral ao obter cost drivers: {str(e)}")
        return pd.DataFrame()


def calculate_new_values(df_resultado: pd.DataFrame, dataframe_costdriver: pd.DataFrame) -> pd.DataFrame:
    """
    Calcula novos valores baseado em cost drivers e variações.
    Calcula Valor_Cif quando ImportExport == 1 usando alpha_FOB2CIF.
    
    Args:
        df_resultado: DataFrame com variações calculadas (deve conter variacao_percentual_FOB, alpha_FOB2CIF e ImportExport)
        dataframe_costdriver: DataFrame com dados de cost drivers
        
    Returns:
        DataFrame com novos valores calculados (Valor e Valor_Cif quando aplicável)
    """
    if dataframe_costdriver.empty:
        logger = logging.getLogger(__name__)
        logger.warning("DataFrame de cost drivers vazio, retornando df_resultado sem modificações")
        # Transformar df_resultado para formato esperado por preencher_lacunas_com_media
        # Converter FOB para Valor e CIF para Valor_Cif se existirem
        if df_resultado.empty:
            return pd.DataFrame()
        
        df_result = df_resultado.copy()
        
        # Verificar se tem FOB ou Valor
        if 'FOB' in df_result.columns:
            df_result['Valor'] = df_result['FOB']
        elif 'Valor' not in df_result.columns:
            logger.warning("DataFrame não contém 'FOB' nem 'Valor', retornando DataFrame vazio")
            return pd.DataFrame()
        
        if 'CIF' in df_result.columns:
            df_result['Valor_Cif'] = df_result['CIF']
        
        # Manter apenas colunas necessárias
        cols = ['ID', 'Data', 'Valor', 'suavisado']
        if 'ImportExport' in df_result.columns:
            cols.append('ImportExport')
        if 'Valor_Cif' in df_result.columns:
            cols.append('Valor_Cif')
        
        # Verificar se todas as colunas necessárias existem
        missing_cols = [col for col in cols if col not in df_result.columns]
        if missing_cols:
            logger.warning(f"Colunas faltando: {missing_cols}, retornando DataFrame vazio")
            return pd.DataFrame()
        
        return df_result[cols]
    
    # Verificar se ImportExport e alpha_FOB2CIF estão disponíveis
    has_importexport = 'ImportExport' in df_resultado.columns
    has_alpha = 'alpha_FOB2CIF' in df_resultado.columns
    
    dfs = []
    for ID, group in dataframe_costdriver.groupby('ID'):
        sub_df = df_resultado[df_resultado['ID'] == ID].copy()
        if sub_df.empty:
            continue
        
        # Obter ImportExport se disponível
        ImportExport = None
        if has_importexport:
            ImportExport = sub_df['ImportExport'].iloc[0]
            
        max_resultado = sub_df['Data'].max()
        max_plataforma = group['Data'].max()
        
        if max_resultado > max_plataforma:
            sub_df = sub_df[sub_df['Data'] > max_plataforma].copy()
            sub_df['variacao_percentual_FOB_cum'] = sub_df['variacao_percentual_FOB'].add(1).cumprod()
            
            ultimo_valor_plataforma = group['Valor'].iloc[-1]
            sub_df['Valor'] = sub_df['variacao_percentual_FOB_cum'] * ultimo_valor_plataforma
            
            # Calcular Valor_Cif apenas para importação usando alpha_FOB2CIF
            if ImportExport == 1 and has_alpha:
                sub_df['Valor_Cif'] = sub_df['alpha_FOB2CIF'] * sub_df['Valor']
            else:
                sub_df['Valor_Cif'] = pd.NA
            
            # Selecionar colunas para retorno
            cols = ['ID', 'Data', 'Valor', 'suavisado']
            if has_importexport:
                cols.append('ImportExport')
            if 'Valor_Cif' in sub_df.columns:
                cols.append('Valor_Cif')
            dfs.append(sub_df[cols])
    
    if not dfs:
        return pd.DataFrame()
    
    df = pd.concat(dfs, ignore_index=True)
    
    df_ultimo = dataframe_costdriver[
            dataframe_costdriver['Data'] == dataframe_costdriver.groupby('ID')['Data'].transform('max')
        ].reset_index(drop=True)
    antes_concat = len(df)
    df = pd.concat([df_ultimo, df], ignore_index=True)
    logger_diag.info(f"[DIAG] após concat df_ultimo: {len(df_ultimo)} + {antes_concat} = {len(df)} linhas")

    # Filtrar apenas IDs com mais de uma data
    antes_count = len(df)
    df = df[df.groupby('ID')['Data'].transform('count') > 1]
    logger_diag.info(f"[DIAG] após filtro count>1: {antes_count} → {len(df)} linhas")
    # Remover linhas onde ambos Valor e Valor_Cif são NaN
    if 'Valor_Cif' in df.columns:
        antes_nan = len(df)
        df = df[~(df['Valor'].isna() & df['Valor_Cif'].isna())]
        logger_diag.info(f"[DIAG] após filtro duplo-NaN: {antes_nan} → {len(df)} linhas")
    
    return df


def preenche_lacuna(df: pd.DataFrame) -> pd.DataFrame:
    """
    Preenche lacunas nos dados usando interpolação linear.
    
    Args:
        df: DataFrame com colunas 'ID', 'Data' e 'Valor'
               Opcionalmente pode conter 'Valor_Cif' e 'ImportExport'
        
    Returns:
        DataFrame com lacunas preenchidas
        Retorna 'Valor_Cif' apenas para importação (ImportExport == 1)
    """
    # Verificar se DataFrame está vazio
    if df.empty:
        return pd.DataFrame()
    
    # Aplica a função para preencher os dados
    df_preenchido = preencher_lacunas_com_media(df)
    
    # Verificar se resultado está vazio
    if df_preenchido.empty:
        return pd.DataFrame()
    
    # Verificar se colunas necessárias existem antes de ordenar
    if 'ID' not in df_preenchido.columns:
        raise ValueError("Coluna 'ID' não encontrada no DataFrame após preencher_lacunas_com_media")
    if 'Data' not in df_preenchido.columns:
        raise ValueError("Coluna 'Data' não encontrada no DataFrame após preencher_lacunas_com_media")
    
    df_preenchido.sort_values(['ID', 'Data'], ascending=[True, True], inplace=True)
    
    # Verificar se Valor_Cif existe antes de processar
    if 'Valor_Cif' in df_preenchido.columns:
        df_preenchido.loc[df_preenchido['Valor_Cif'] < df_preenchido['Valor'], 'Valor_Cif'] = pd.NA
        # Verificar se ImportExport existe antes de usar
        if 'ImportExport' in df_preenchido.columns:
            df_preenchido.loc[df_preenchido['ImportExport'] == 0, 'Valor_Cif'] = pd.NA
    
    # Selecionar colunas: sempre ID, Data, Valor; Valor_Cif apenas quando existir
    cols = ['ID', 'Data', 'Valor']
    if 'Valor_Cif' in df_preenchido.columns:
        cols.append('Valor_Cif')
    df_preenchido = df_preenchido[cols]
    
    return df_preenchido


def upload_comex(df_preenchido: pd.DataFrame, ExcluirHistorico: str = 'N', percentualOutlier: float = 60, logger=None) -> bool:
    """
    Função mockada para upload de dados COMEX.
    Apenas loga o que seria enviado, sem fazer requisições reais.
    
    Args:
        df_preenchido: DataFrame com dados para upload (deve conter ID, Data, Valor, opcionalmente Valor_Cif)
        ExcluirHistorico: 'N' ou 'S' para excluir histórico (padrão: 'N')
        percentualOutlier: Percentual de outlier (padrão: 60)
        logger: Logger opcional para logging
        
    Returns:
        True (sucesso simulado)
    """
    if logger is None:
        logger = logging.getLogger(__name__)
    
    endpoint_cost = 'https://api-costdrivers.gep.com/costdrivers-api'
    opc = 9
    idioma = 1
    identity = '2258FB7E-7F19-483E-BBAE-8250973D3658'
    merchantID = ''
    
    logger.info("=== INICIANDO UPLOAD MOCKADO ===")
    logger.info(f"Total de IDs para upload: {df_preenchido['ID'].nunique()}")
    logger.info(f"Total de registros: {len(df_preenchido)}")
    logger.info(f"ExcluirHistorico: {ExcluirHistorico}")
    logger.info(f"PercentualOutlier: {percentualOutlier}")
    
    # Formatar Data como "01-MM-YYYY"
    df_upload = df_preenchido.copy()
    df_upload['Data'] = pd.to_datetime(df_upload['Data']).dt.strftime("01-%m-%Y")
    
    # Processar cada ID
    for ID, group in df_upload.groupby('ID'):
        # Preparar dados para JSON
        json_df = group.dropna(axis=1, how='all').copy()
        
        # proteger subir cif vazio
        if 'Valor_Cif' in json_df.columns:
            json_df.dropna(subset=['Valor_Cif'], inplace=True)
            
        json_df['Base'] = 0
        json_df['Explicativa'] = 0
        
        # Converter para JSON
        json_data = json_df.to_json(orient='records').replace("'", '"')
        
        # Criar jsonSeq
        jsonSeq = str({
            'ID': str(ID),
            'ExcluirHistorico': ExcluirHistorico,
            'Origem': 'Data Science'
        }).replace("'", '"')
        
        # Criar estrutura de requisição
        rjson = {
            'opc': opc,
            'idioma': idioma,
            'identity': identity,
            'percentualOutlier': percentualOutlier,
            'merchantID': merchantID,
            'json': json_data,
            'jsonSeq': jsonSeq
        }
        
        # Logar informações (sem fazer requisição real)
        logger.info(f"ID: {ID} - Registros: {len(group)}")
        logger.debug(f"URL que seria chamada: {endpoint_cost}/api/v1/DataScience/UpdateOption-9")
        logger.debug(f"JSON que seria enviado: {json_data[:200]}...")  # Primeiros 200 chars
        
        # Em produção, aqui faria:
        # lista_requisicao.append({
        #     'url': f'{endpoint_cost}/api/v1/DataScience/UpdateOption-9',
        #     'method': 'put',
        #     'data': rjson,
        #     'ID': ID
        # })
    
    logger.info("=== UPLOAD MOCKADO CONCLUÍDO (nenhuma requisição real foi feita) ===")
    return True


def upload_data(df: pd.DataFrame, ExcluirHistorico: str = 'N', percentualOutlier: float = 60) -> bool:
    """
    Realiza upload dos dados para o destino final (mockado).
    
    Args:
        df: DataFrame com dados para upload
        ExcluirHistorico: 'N' ou 'S' para excluir histórico (padrão: 'N')
        percentualOutlier: Percentual de outlier (padrão: 60)
        
    Returns:
        True se mockado com sucesso
    """
    logger = logging.getLogger(__name__)
    logger.info("Usando função de upload mockada")
    
    return upload_comex(df, ExcluirHistorico=ExcluirHistorico, percentualOutlier=percentualOutlier, logger=logger)