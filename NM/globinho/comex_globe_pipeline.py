"""
Pipeline base para processamento COMEX Globe.
Classe abstrata que define a estrutura comum para todos os processadores de países.
"""
import json
import os
import sys
import logging
from pathlib import Path
from abc import ABC, abstractmethod
from typing import Dict, List, Any, Iterable
from datetime import datetime
import asyncio
import aiohttp
import numpy as np
import pandas as pd
import polars as pl
import requests
from library.costdrivers import *

# Configurar logger
logger = logging.getLogger(__name__)


class BaseComexProcessor(ABC):
    """
    Classe base abstrata para processamento de dados COMEX.
    Define o fluxo comum usando Polars para ingestão/transformação e pandas para upload.
    """
    
    def __init__(self, config_path: str = 'config_comex.json'):
        """
        Inicializa o processador carregando configurações.
        
        Args:
            config_path: Caminho para o arquivo de configuração JSON
            
        Raises:
            FileNotFoundError: Se arquivo de configuração não for encontrado
            json.JSONDecodeError: Se arquivo JSON estiver malformado
        """
        # Tentar encontrar o arquivo de configuração em múltiplos locais
        config_file = self._find_config_file(config_path)
        
        try:
            with open(config_file, 'r', encoding='utf-8') as f:
                self.config = json.load(f)
        except FileNotFoundError:
            raise FileNotFoundError(f"Arquivo de configuração não encontrado: {config_path}")
        except json.JSONDecodeError as e:
            raise json.JSONDecodeError(f"Erro ao decodificar JSON em {config_file}: {str(e)}", e.doc, e.pos)
        
        # Inicializar atributos após carregar configuração
        self.endpoint_cost = self.config['api']['endpoint']
        self.columns_typing = {
            "dataNCM": str,
            "idIndicador": int,
            "ie": int,
            "idPais": int,
            "quantidade": float,
            "peso": float,
            "fob": float,
            "frete": float,
            "seguro": float,
            "valorCIF": float,
            "unid": str,
            "via": str,
            "uf": str
        }
        self.df_merged = None
    
    def _find_config_file(self, config_path: str) -> Path:
        """
        Procura o arquivo de configuração em múltiplos locais possíveis.
        
        Args:
            config_path: Caminho fornecido (pode ser relativo ou absoluto)
            
        Returns:
            Path do arquivo encontrado
            
        Raises:
            FileNotFoundError: Se arquivo não for encontrado em nenhum local
        """
        return self._find_file(config_path, file_type="configuração")
    
    def _find_file(self, file_path: str, file_type: str = "arquivo") -> Path:
        """
        Procura um arquivo em múltiplos locais possíveis.
        
        Args:
            file_path: Caminho fornecido (pode ser relativo ou absoluto)
            file_type: Tipo do arquivo para mensagens de erro (ex: "configuração", "IDs", "dados")
            
        Returns:
            Path do arquivo encontrado
            
        Raises:
            FileNotFoundError: Se arquivo não for encontrado em nenhum local
        """
        # Se o path fornecido for absoluto e existir, usar diretamente
        if Path(file_path).is_absolute() and Path(file_path).exists():
            return Path(file_path)
        
        # Lista de possíveis locais para procurar o arquivo
        possible_paths = []
        
        # 1. Diretório atual (onde o script está sendo executado)
        possible_paths.append(Path(file_path))
        
        # 2. Diretório NM/globinho/ (onde alguns arquivos estão)
        current_file_dir = Path(__file__).parent
        possible_paths.append(current_file_dir / file_path)
        
        # 3. Diretório library/ (onde IDS_comex.xlsx está)
        project_root = current_file_dir.parent.parent.parent
        possible_paths.append(project_root / "library" / file_path)
        
        # 4. Diretório NM/globinho/ com subdiretórios específicos
        possible_paths.append(current_file_dir / file_path)
        
        # 5. Raiz do projeto
        possible_paths.append(project_root / file_path)
        
        # 6. Path relativo NM/globinho/
        possible_paths.append(Path("NM/globinho") / file_path)
        
        # 7. Path relativo library/
        possible_paths.append(Path("library") / file_path)
        
        # 8. Se o arquivo está em um subdiretório do globinho (ex: ARG/paisID_ARG.csv)
        # Tentar encontrar no diretório do país específico
        for country_dir in current_file_dir.iterdir():
            if country_dir.is_dir() and not country_dir.name.startswith('_'):
                possible_paths.append(country_dir / file_path)
        
        # Tentar cada path até encontrar o arquivo
        for path in possible_paths:
            if path.exists():
                logger.debug(f"Arquivo {file_type} encontrado: {path.absolute()}")
                return path
        
        # Se não encontrou, levantar erro
        raise FileNotFoundError(
            f"Arquivo {file_type} não encontrado em nenhum dos locais tentados:\n" +
            "\n".join([f"  - {p.absolute()}" for p in possible_paths[:10]])  # Limitar a 10 para não poluir
        )
    
    @abstractmethod
    def load_raw_data(self) -> pl.LazyFrame:
        """
        Carrega dados brutos específicos do país usando Polars.
        
        Returns:
            pl.LazyFrame: Dados brutos carregados
        """
        pass
    
    @abstractmethod
    def transform_data(self, lf: pl.LazyFrame) -> pl.LazyFrame:
        """
        Aplica transformações específicas do país em Polars.
        
        Args:
            lf: LazyFrame com dados brutos
            
        Returns:
            pl.LazyFrame: Dados transformados
        """
        pass
    
    @abstractmethod
    def normalize_countries(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Normaliza IDs de países usando tabela de mapeamento.
        Recebe pandas DataFrame após conversão final.
        
        Args:
            df: DataFrame pandas com dados processados
            
        Returns:
            pd.DataFrame: DataFrame com países normalizados
        """
        pass
    
    def load_indicator_ids(self, country_name: str) -> pl.DataFrame:
        """
        Carrega IDs de indicadores do Excel e converte para Polars.
        
        Args:
            country_name: Nome do país conforme config
            
        Returns:
            pl.DataFrame: DataFrame Polars com IDs de indicadores
            
        Raises:
            FileNotFoundError: Se arquivo Excel não for encontrado
            KeyError: Se colunas esperadas não existirem no arquivo
        """
        try:
            ids_path_str = self.config['paths']['ids_excel']
            ids_path = self._find_file(ids_path_str, file_type="de IDs")
            
            ids_pd = pd.read_excel(ids_path)
            
            # Validar colunas necessárias
            required_cols = ['Pais_1', 'IDIndicePrincipal', 'NCM', 'ImportExport']
            missing_cols = [col for col in required_cols if col not in ids_pd.columns]
            if missing_cols:
                raise KeyError(f"Colunas faltando no arquivo {ids_path}: {missing_cols}")
            
            ids_pd = ids_pd[ids_pd['Pais_1'] == country_name]
            if ids_pd.empty:
                logger.warning(f"Nenhum ID encontrado para país '{country_name}' em {ids_path}")
            
            ids_pd = ids_pd.sort_values(by=['IDIndicePrincipal'])
            ids_pd = ids_pd.drop_duplicates(subset=['NCM', 'ImportExport'], keep='first')
            
            # Converter para Polars
            ids_pl = pl.from_pandas(ids_pd[['IDIndicePrincipal', 'NCM', 'ImportExport']])
            ids_pl = ids_pl.rename({'NCM': 'ncm'})
            
            return ids_pl
        except Exception as e:
            error_msg = f"Erro ao carregar IDs de indicadores para {country_name}: {str(e)}"
            logger.error(error_msg, exc_info=True)
            raise
    
    def merge_with_indicators(self, lf: pl.LazyFrame, ids: pl.DataFrame) -> pl.LazyFrame:
        """
        Faz merge dos dados com IDs de indicadores usando Polars.
        
        Args:
            lf: LazyFrame com dados transformados
            ids: DataFrame Polars com IDs de indicadores
            
        Returns:
            pl.LazyFrame: Dados com IDs de indicadores mergeados
        """
        # Garantir tipos compatíveis para join
        ids_lazy = ids.lazy()
        
        # Converter tipos para garantir compatibilidade
        # ncm: converter para string se necessário (mais flexível)
        # ImportExport: converter para Int8 (padrão)
        
        # Converter ncm para string em ambos (mais seguro para join)
        lf = lf.with_columns([
            pl.col('ncm').cast(pl.Utf8).alias('ncm')
        ])
        ids_lazy = ids_lazy.with_columns([
            pl.col('ncm').cast(pl.Utf8).alias('ncm')
        ])
        
        # Converter ImportExport para Int8 em ambos
        lf = lf.with_columns([
            pl.col('ImportExport').cast(pl.Int8).alias('ImportExport')
        ])
        ids_lazy = ids_lazy.with_columns([
            pl.col('ImportExport').cast(pl.Int8).alias('ImportExport')
        ])
        
        # Fazer join
        lf_merged = lf.join(
            ids_lazy,
            on=['ncm', 'ImportExport'],
            how='inner'
        )
        
        # Renomear
        lf_merged = lf_merged.rename({
            'IDIndicePrincipal': 'idIndicador'
        })
        
        return lf_merged
    
    def group_data(self, lf: pl.LazyFrame) -> pl.LazyFrame:
        """
        Agrupa dados por dataNCM, idIndicador e idPais usando Polars.
        
        Args:
            lf: LazyFrame com dados mergeados
            
        Returns:
            pl.LazyFrame: Dados agrupados
        """
        # Verificar se colunas de agrupamento existem usando collect_schema() para evitar PerformanceWarning
        group_cols = ['dataNCM', 'idIndicador', 'idPais']
        schema_names = lf.collect_schema().names()
        missing_cols = [col for col in group_cols if col not in schema_names]
        if missing_cols:
            raise ValueError(f"Colunas de agrupamento faltando: {missing_cols}")
        
        # Colunas numéricas para somar
        numeric_cols = ['peso', 'fob', 'frete', 'seguro']
        # Filtrar apenas colunas que existem
        existing_numeric = [col for col in numeric_cols if col in schema_names]
        
        if not existing_numeric:
            raise ValueError("Nenhuma coluna numérica encontrada para agregação")
        
        agg_exprs = [pl.sum(col).alias(col) for col in existing_numeric]
        
        lf_grouped = lf.group_by(group_cols).agg(agg_exprs)
        
        return lf_grouped
    
    def convert_to_pandas(self, lf: pl.LazyFrame) -> pd.DataFrame:
        """
        Converte LazyFrame Polars para DataFrame pandas.
        
        Args:
            lf: LazyFrame Polars
            
        Returns:
            pd.DataFrame: DataFrame pandas
        """
        df = lf.collect().to_pandas()
        
        # Converter dataNCM para string se for datetime
        if 'dataNCM' in df.columns and pd.api.types.is_datetime64_any_dtype(df['dataNCM']):
            df['dataNCM'] = df['dataNCM'].dt.strftime('%Y-%m-%d')
        
        return df
    
    def prepare_for_upload(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Prepara DataFrame para upload garantindo colunas do columns_typing.
        
        Args:
            df: DataFrame pandas processado
            
        Returns:
            pd.DataFrame: DataFrame preparado para upload
        """
        df_prepared = df.copy()
        
        # Garantir todas as colunas do columns_typing
        for col, dtype in self.columns_typing.items():
            if col not in df_prepared.columns:
                df_prepared[col] = np.nan
        
        # Reordenar colunas conforme columns_typing
        df_prepared = df_prepared[list(self.columns_typing.keys())]
        
        # Remover colunas totalmente NaN
        df_prepared = df_prepared.dropna(axis=1, how='all')
        
        # Regra: se soma frete e seguro == 0, setar NaN
        if 'frete' in df_prepared.columns and 'seguro' in df_prepared.columns:
            if (df_prepared['frete'].sum() == 0) and (df_prepared['seguro'].sum() == 0):
                df_prepared['frete'] = np.nan
                df_prepared['seguro'] = np.nan
        
        return df_prepared
    
    def get_token_costdrivers(self) -> str:
        """
        Obtém token de autenticação da API CostDrivers.
        Lê credenciais de variáveis de ambiente por segurança.
        
        Variáveis de ambiente esperadas:
        - COSTDRIVERS_API_KEY: Chave da API
        - COSTDRIVERS_API_EMAIL: Email para autenticação
        - COSTDRIVERS_API_PASSWORD: Senha para autenticação
        
        Returns:
            str: Token de autenticação
            
        Raises:
            ValueError: Se alguma variável de ambiente não estiver configurada
            requests.RequestException: Se requisição HTTP falhar
        """
        try:
            # Ler credenciais de variáveis de ambiente
            api_key = os.getenv('COSTDRIVERS_API_KEY') or self.config.get('api', {}).get('key')
            api_email = os.getenv('COSTDRIVERS_API_EMAIL') or self.config.get('api', {}).get('email')
            api_password = os.getenv('COSTDRIVERS_API_PASSWORD') or self.config.get('api', {}).get('password')
            
            if not api_key or not api_email or not api_password:
                raise ValueError(
                    "Credenciais da API não encontradas. Configure as variáveis de ambiente: "
                    "COSTDRIVERS_API_KEY, COSTDRIVERS_API_EMAIL, COSTDRIVERS_API_PASSWORD"
                )
            
            headers = {
                'key': api_key,
                'email': api_email,
                'pass': api_password
            }
            req = {
                'url': f'{self.endpoint_cost}/api/v1/Auth',
                'method': 'GET'
            }
            logger.debug(f'Requisição para obter token: {req["url"]}')
            
            response = requests.get(req['url'], headers=headers, timeout=30)
            response.raise_for_status()  # Levanta exceção se status não for 2xx
            
            logger.debug(f'Resposta da API: status {response.status_code}')
            resp_data = response.json()
            
            if 'result' not in resp_data or 'tokenCode' not in resp_data['result']:
                raise ValueError("Resposta da API não contém token válido")
            
            token = resp_data['result']['tokenCode']
            logger.info('Token obtido com sucesso')
            return token
            
        except requests.RequestException as e:
            error_msg = f"Erro na requisição HTTP para obter token: {str(e)}"
            logger.error(error_msg, exc_info=True)
            raise requests.RequestException(error_msg) from e
        except (KeyError, ValueError) as e:
            error_msg = f"Erro ao processar resposta da API: {str(e)}"
            logger.error(error_msg, exc_info=True)
            raise ValueError(error_msg) from e
    
    def generate_batches_from_df(
        self,
        df_merged: pd.DataFrame,
        batch_size: int = 20
    ) -> Iterable[Dict]:
        """
        Generator que itera por idIndicador e produz batches de registros.
        
        Args:
            df_merged: DataFrame pandas preparado
            batch_size: Tamanho do batch
            
        Yields:
            Dict: Dict com idIndicador e payload (lista de registros)
        """
        for idIndicador in df_merged['idIndicador'].unique():
            sub_df = df_merged[df_merged['idIndicador'] == idIndicador].copy()
            
            # Garantir colunas presentes
            for col in self.columns_typing.keys():
                if col not in sub_df.columns:
                    sub_df[col] = np.nan
            
            # Reordenar colunas conforme columns_typing
            sub_df = sub_df[list(self.columns_typing.keys())]
            
            # Remover colunas totalmente NaN
            sub_df = sub_df.dropna(axis=1, how='all')
            
            # Regra: se soma frete e seguro == 0, setar NaN
            if 'frete' in df_merged.columns and 'seguro' in df_merged.columns:
                if (df_merged['frete'].sum() == 0) and (df_merged['seguro'].sum() == 0):
                    if 'frete' in sub_df.columns:
                        sub_df['frete'] = np.nan
                    if 'seguro' in sub_df.columns:
                        sub_df['seguro'] = np.nan
            
            # Converter em lista de dicts
            records = sub_df.to_dict(orient='records')
            
            # Emitir em batches de batch_size
            batch: List[Dict] = []
            for rec in records:
                batch.append(rec)
                if len(batch) >= batch_size:
                    yield {'idIndicador': idIndicador, 'payload': batch}
                    batch = []
            if batch:
                yield {'idIndicador': idIndicador, 'payload': batch}
    
    async def post_single_batch(
        self,
        session: aiohttp.ClientSession,
        batch_payload: Dict,
        headers: Dict[str, str],
        semaphore: asyncio.Semaphore
    ) -> Dict[str, Any]:
        """
        Envia um batch para o endpoint e retorna resultado.
        
        Args:
            session: Sessão aiohttp
            batch_payload: Dict com idIndicador e payload
            headers: Headers HTTP
            semaphore: Semáforo para controle de concorrência
            
        Returns:
            Dict: Resultado do post
        """
        url = f"{self.endpoint_cost}/api/v1/InternationalTrade"
        json_body = batch_payload['payload']
        
        async with semaphore:
            try:
                timeout = aiohttp.ClientTimeout(total=self.config['upload']['timeout'])
                async with session.post(url, json=json_body, headers=headers, timeout=timeout) as resp:
                    text = await resp.text()
                    status = resp.status
                    logger.debug(f'Batch enviado para idIndicador {batch_payload["idIndicador"]} - Status: {status}')
                    
                    try:
                        data = await resp.json()
                    except Exception as json_error:
                        logger.warning(f'Erro ao decodificar JSON da resposta para idIndicador {batch_payload["idIndicador"]}: {json_error}, usando texto')
                        data = text
                    
                    if status != 200:
                        logger.warning(f'Batch falhou para idIndicador {batch_payload["idIndicador"]}: Status {status}')
                    
                    return {
                        'idIndicador': batch_payload['idIndicador'],
                        'status': status,
                        'response': data
                    }
            except asyncio.TimeoutError as e:
                error_msg = f"Timeout ao enviar batch para idIndicador {batch_payload['idIndicador']}: {str(e)}"
                logger.error(error_msg)
                return {
                    'idIndicador': batch_payload['idIndicador'],
                    'status': 'timeout',
                    'response': error_msg
                }
            except aiohttp.ClientError as e:
                error_msg = f"Erro de cliente HTTP ao enviar batch para idIndicador {batch_payload['idIndicador']}: {str(e)}"
                logger.error(error_msg)
                return {
                    'idIndicador': batch_payload['idIndicador'],
                    'status': 'error',
                    'response': error_msg
                }
            except Exception as e:
                error_msg = f"Erro inesperado ao enviar batch para idIndicador {batch_payload['idIndicador']}: {str(e)}"
                logger.error(error_msg, exc_info=True)
                return {
                    'idIndicador': batch_payload['idIndicador'],
                    'status': 'error',
                    'response': error_msg
                }
    
    async def upload_batches(
        self,
        df_merged: pd.DataFrame,
        headers: Dict[str, str],
        batch_size: int = None,
        workers: int = None
    ) -> List[Dict[str, Any]]:
        """
        Consome generate_batches_from_df e dispara post_single_batch em paralelo.
        
        Args:
            df_merged: DataFrame pandas preparado
            headers: Headers HTTP
            batch_size: Tamanho do batch (usa config se None)
            workers: Número de workers (usa config se None)
            
        Returns:
            List[Dict]: Lista de resultados por batch
        """
        if batch_size is None:
            batch_size = self.config['upload']['batch_size']
        if workers is None:
            workers = self.config['upload']['workers']
        
        semaphore = asyncio.Semaphore(workers)
        results: List[Dict[str, Any]] = []
        
        connector = aiohttp.TCPConnector(limit_per_host=workers)
        timeout = aiohttp.ClientTimeout(total=300)
        
        async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
            tasks: List[asyncio.Task] = []
            max_pending_tasks = workers * 4
            
            for batch_payload in self.generate_batches_from_df(df_merged, batch_size):
                task = asyncio.create_task(
                    self.post_single_batch(session, batch_payload, headers, semaphore)
                )
                tasks.append(task)
                
                if len(tasks) >= max_pending_tasks:
                    done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
                    for d in done:
                        try:
                            results.append(d.result())
                        except Exception as e:
                            results.append({'status': 'error', 'response': str(e)})
                    tasks = list(pending)
                
                # break  # Remover esta linha para processar todos os batches

            if tasks:
                done_results = await asyncio.gather(*tasks, return_exceptions=True)
                # print('done_results: ', done_results)
                for r in done_results:
                    if isinstance(r, Exception):
                        results.append({'status': 'error', 'response': str(r)})
                    else:
                        results.append(r)
        
        return results
    
    def run_upload(self, df_merged: pd.DataFrame) -> List[Dict[str, Any]]:
        """
        Executa upload de forma síncrona usando asyncio.
        
        Args:
            df_merged: DataFrame pandas preparado
            
        Returns:
            List[Dict]: Resultados do upload com status e respostas de cada batch
        """
        headers = {
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {self.token}',
            'Cookie': 'Idioma=2'
        }

        if df_merged.empty:
            logger.info("DataFrame vazio, nenhum upload será realizado")
            return []
        
        logger.info(f'Iniciando upload de {len(df_merged)} registros...')
        try:
            # Executar upload assíncrono de forma síncrona
            # Usar configurações do config ou valores padrão
            batch_size = self.config.get('upload', {}).get('batch_size', 5)
            workers = self.config.get('upload', {}).get('workers', 2)
            
            results = asyncio.run(self.upload_batches(df_merged, headers, batch_size=batch_size, workers=workers))
            logger.info(f'Upload concluído! Total de batches: {len(results)}')
            successful = sum(1 for r in results if r.get('status') == 200)
            failed = len(results) - successful
            logger.info(f'Batches bem-sucedidos: {successful}, Falhas: {failed}')
            return results
        except Exception as e:
            error_msg = f"Erro durante upload: {str(e)}"
            logger.error(error_msg, exc_info=True)
            return [{'status': 'error', 'response': error_msg}]
    
    def get_country_name(self) -> str:
        """
        Obtém o nome do país da configuração baseado no nome da classe.
        
        Returns:
            str: Nome do país
        """
        class_name = self.__class__.__name__
        # Remover prefixo COMEX_ e sufixo _GLOBE
        country_key = class_name.replace('COMEX_', '').replace('_GLOBE', '').lower()
        return self.config['countries'][country_key]
    
    def get_costdrivers_data(self, df: pd.DataFrame, n_months: int = 3) -> pd.DataFrame:
        """
        Obtém dados históricos de cost drivers da API para validação de novos registros.
        
        Args:
            df: DataFrame com IDs de indicadores (deve conter coluna 'IDIndicePrincipal')
            n_months: Número de meses para buscar dados históricos (padrão: 3)
            
        Returns:
            pd.DataFrame: DataFrame com dados históricos de cost drivers
        """
        def _make_lista_requisicao(lista_ids: list, data_min: str, data_max: str) -> List[Dict]:
            """
            Cria lista de requisições para a API de cost drivers.
            Divide IDs em batches de 49 (limite da API).
            """
            total_ids = len(lista_ids)
            lista_requisicao = []
            batch_size = 49
            
            for i in range(0, total_ids, batch_size):
                dict_req = {
                    'url': f'{self.endpoint_cost}/api/v1/DataScience/option/11',
                    'method': 'get',
                    'params': {
                        'ids': ','.join(list(map(str, lista_ids[i:i + batch_size]))),
                        'dataCalculo1': data_min,
                        'dataCalculo2': data_max
                    }
                }
                lista_requisicao.append(dict_req)
            
            return lista_requisicao
        
        # Obter token usando método unificado da classe
        logger.info("Buscando dados de cost drivers da API...")
        self.token = self.get_token_costdrivers()
        
        # Preparar lista de IDs e datas
        lista_ids = df['IDIndicePrincipal'].unique().tolist()
        if not lista_ids:
            logger.warning("Nenhum ID encontrado para buscar dados de cost drivers")
            return pd.DataFrame()
        
        # Calcular intervalo de datas (n_months antes até hoje)
        data_min = (datetime.now() - pd.DateOffset(months=n_months)).strftime("%d-%m-%Y")
        data_max = datetime.now().strftime("%d-%m-%Y")
        
        # Criar requisições em batches
        lista_requisicao = _make_lista_requisicao(lista_ids, data_min, data_max)
        
        # Executar requisições usando ApiAsync
        headers = {'Authorization': f'Bearer {self.token}'}
        dados_costdrivers = ApiAsync(True, lista_requisicao, headers=headers).run()
        
        # Processar respostas e construir DataFrame
        lista_dataframe = [
            pd.DataFrame(rsp['result'])
            for rsp in dados_costdrivers
            if 'Error' not in rsp.keys() and 'result' in rsp
        ]
        
        if not lista_dataframe:
            logger.warning("Nenhum dado retornado da API de cost drivers")
            return pd.DataFrame()
        
        dataframe_costdriver = pd.concat(lista_dataframe, ignore_index=True)
        
        # Filtrar apenas registros com base == 0 e selecionar colunas necessárias
        dataframe_costdriver = dataframe_costdriver.query("base == 0")
        dataframe_costdriver = dataframe_costdriver[['indicePrincipalID', 'dataIndice', 'valor']]
        dataframe_costdriver.columns = ['IDIndicePrincipal', 'Data', 'Valor']
        dataframe_costdriver['Data'] = pd.to_datetime(dataframe_costdriver['Data'], format='%Y-%m-%dT%H:%M:%S')
        
        logger.info(f"Dados de cost drivers obtidos com sucesso: {len(dataframe_costdriver)} registros")
        
        return dataframe_costdriver
    
    def validate_new_data(self, dataframe_costdriver: pd.DataFrame, df_pandas: pd.DataFrame) -> pd.DataFrame:
        """
        Valida se há dados novos para upload comparando com dados existentes.
        
        Args:
            dataframe_costdriver: DataFrame com dados atuais de cost drivers
            df_pandas: DataFrame pandas preparado para upload
            
        Returns:
            pd.DataFrame: DataFrame com dados novos para upload ou vazio se não houver
        """
        if dataframe_costdriver.empty:
            logger.info("Nenhum dado existente encontrado na API de cost drivers.")
            return df_pandas  # Nenhum dado existente, então tudo é novo
        
        print("dataframe_costdriver:\n", dataframe_costdriver.head())
        print(dataframe_costdriver.info())
        
        print("df_pandas:\n", df_pandas.head())
        print(df_pandas.info())
        
        df_pandas['dataNCM'] = pd.to_datetime(df_pandas['dataNCM'], format='%Y-%m-%d')
        
        platform_max_date = dataframe_costdriver.groupby('IDIndicePrincipal')['Data'].max().reset_index()
        collection_max_date = df_pandas.groupby('idIndicador')['dataNCM'].max().reset_index()
        
        platform_ids = set(platform_max_date['IDIndicePrincipal'].unique())
        
        new_data = []
        for ID in collection_max_date['idIndicador'].unique():
            print('Checking ID: ', ID)
            if ID in platform_ids:
                collection_date = collection_max_date.loc[collection_max_date['idIndicador'] == ID, 'dataNCM'].values[0]
                print("collection_date: ", collection_date)
                platform_date = platform_max_date.loc[platform_max_date['IDIndicePrincipal'] == ID, 'Data'].values[0]
                print("platform_date: ", platform_date)
                if collection_date >= platform_date:
                    print(f"ID {ID} is new data (collection_date >= platform_date)")
                    new_data.append(ID)
        
        if not new_data:
            return pd.DataFrame()
        
        df_pandas = df_pandas[df_pandas['idIndicador'].isin(new_data)]
        df_pandas['dataNCM'] = df_pandas['dataNCM'].dt.strftime('%Y-%m-%d')
        logger.info(f"Registros novos para upload: {len(df_pandas)} registros")
        return df_pandas
        
    
    def process(self) -> List[Dict[str, Any]]:
        """
        Método principal que orquestra todo o processamento.
        Valida se há dados novos comparando com dados existentes na API antes de fazer upload.
        
        Returns:
            List[Dict]: Resultados do upload
        """
        # 1. Carregar dados brutos
        print("init # 1. Carregar dados brutos")
        lf_raw = self.load_raw_data()
        
        # 2. Transformar dados
        print("init # 2. Transformar dados")
        lf_transformed = self.transform_data(lf_raw)
        
        # 3. Carregar IDs de indicadores
        print("init # 3. Carregar IDs de indicadores")
        country_name = self.get_country_name()
        ids = self.load_indicator_ids(country_name)
        print('ids:\n', ids.head())
        
        # 4. Merge com indicadores
        print("init # 4. Merge com indicadores")
        lf_merged = self.merge_with_indicators(lf_transformed, ids)
        
        # 5. Agrupar dados
        print("init # 5. Agrupar dados")
        lf_grouped = self.group_data(lf_merged)
        
        # 6. Converter para pandas
        print("init # 6. Converter para pandas")
        df_pandas = self.convert_to_pandas(lf_grouped)
        print('df_pandas:\n', df_pandas.head())
        
        # 7. Validar se há dados novos comparando com dados existentes na API
        print("init # 7. Validar se há dados novos comparando com dados existentes na API")
        dataframe_costdriver = self.get_costdrivers_data(ids.to_pandas(), n_months=3)
        print('dataframe_costdriver:\n', dataframe_costdriver.head())
        new_data_df = self.validate_new_data(dataframe_costdriver, df_pandas)
        print('new_data_df:\n', new_data_df.head())
        
        if new_data_df.empty:
            logger.info("Processamento encerrado: nenhum dado novo para upload.")
            self.df_merged = df_pandas
            return []
        else:
            self.df_merged = new_data_df
            logger.info(f"Dados novos encontrados: {len(new_data_df)} registros para upload.")
        
            # 8. Normalizar países
            df_normalized = self.normalize_countries(new_data_df)
            print('df_normalized:\n', df_normalized.head())
            
            # 9. Preparar para upload
            df_prepared = self.prepare_for_upload(df_normalized)
            print('df_prepared:\n', df_prepared.head())
            
            # 10. Upload
            results = self.run_upload(df_prepared)
            
            self.df_merged = df_prepared
            return results