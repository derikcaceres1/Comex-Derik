from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import smtplib

from io import StringIO, BytesIO
import yaml
import json
import ast
import asyncio
import random
import ssl
import os
from datetime import date, datetime, timedelta
import re
import aiohttp
import certifi
import pandas as pd
import requests
from unidecode import unidecode
import numpy as np
from pandas.tseries.offsets import BDay
from library.BlobStorage_API import AzureBlobStorage

# Carrega as variáveis do .env
from dotenv import load_dotenv

# Tentar carregar .env da raiz do projeto
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
env_path = os.path.join(project_root, '.env')
if os.path.exists(env_path):
    load_dotenv(dotenv_path=env_path)
else:
    # Fallback: tentar carregar de settings.ENV na mesma pasta da library
    settings_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'settings.ENV')
    if os.path.exists(settings_path):
        load_dotenv(dotenv_path=settings_path)
    else:
        # Último fallback: tentar carregar .env do diretório atual
        load_dotenv()

# Credenciais e endpoint via variáveis de ambiente
# Variáveis esperadas: COSTDRIVERS_PASSWORD, COSTDRIVERS_ENDPOINT
pass_word = os.getenv('COSTDRIVERS_PASSWORD')
if not pass_word:
    raise ValueError("Variável de ambiente COSTDRIVERS_PASSWORD não encontrada. Configure no arquivo .env ou settings.ENV")

endpoint_cost = os.getenv('COSTDRIVERS_ENDPOINT', 'https://api-costdrivers.gep.com/costdrivers-api')

def get_airflow_include_path():
    """
    Retorna o path do diretório include do Airflow.
    Usa variável de ambiente AIRFLOW_INCLUDE_PATH se disponível,
    caso contrário detecta automaticamente se está rodando no Airflow.
    
    Returns:
        str: Path do diretório include
    """
    airflow_path = os.getenv('AIRFLOW_INCLUDE_PATH')
    if airflow_path:
        return airflow_path
    
    if "/opt/airflow" in os.getcwd():
        return "/opt/airflow/include/"
    else:
        return os.path.dirname(os.path.abspath(__file__))

path_dir = get_airflow_include_path()
    
dotenv_path = os.path.join(path_dir, 'settings.ENV')
load_dotenv(dotenv_path=dotenv_path)

class matrix_validation:

    def __init__(self, lista_ids) -> None:
        self.lista_ids = lista_ids
        self.dataframe_costdriver = self.get_N_months(lista_ids=self.lista_ids)

    def get_N_months(self, lista_ids: list, N=12):
        data_max = datetime.now()
        days = 31 * N
        data_min = data_max - timedelta(days)
        data_max = data_max.strftime('%d-%m-%Y')
        data_min = data_min.strftime('%d-%m-%Y')
        total_ids = len(lista_ids)
        lista_requisicao = []
        for i in range(0, total_ids, 49):
            dict_ = {
                'url': f'{endpoint_cost}/api/v1/DataScience/option/11', 'method': 'get'}
            if (total_ids - i) < 49:
                dict_['params'] = {'ids': ','.join(list(map(
                    str, lista_ids[i:]))), 'dataCalculo1': data_min, 'dataCalculo2': data_max}  # type: ignore
            else:
                dict_['params'] = {'ids': ','.join(list(map(
                    str, lista_ids[i:i + 49]))), 'dataCalculo1': data_min, 'dataCalculo2': data_max}  # type: ignore
            lista_requisicao.append(dict_)
        token_costdriver = get_token_costdrivers()
        headers = {'Authorization': 'Bearer ' + token_costdriver}
        dados_costdrivers = ApiAsync(True, lista_requisicao, headers=headers).run()
        lista_dataframe = [pd.DataFrame(rsp['result']) for rsp in dados_costdrivers if (not 'Error' in rsp.keys())]
        dataframe_costdriver = pd.concat(lista_dataframe, ignore_index=True)
        dataframe_costdriver = dataframe_costdriver.query("base == 0")
        dataframe_costdriver = dataframe_costdriver[['indicePrincipalID', 'dataIndice', 'valor']]
        dataframe_costdriver.columns = ['ID', 'Data', 'Valor']
        dataframe_costdriver['Data'] = pd.to_datetime(dataframe_costdriver['Data'], format='%Y-%m-%dT%H:%M:%S')
        dataframe_costdriver = dataframe_costdriver.astype(
            {'ID': 'int32', 'Data': 'datetime64[ns]', 'Valor': 'float32'})
        return dataframe_costdriver

    def truth_matrix(self, dataframe_costdriver):
        self.matriz_verdade = dict()
        for ID, grupo_cost in dataframe_costdriver.groupby(['ID'], as_index=False):
            self.matriz_verdade[ID[0]] = {}
            self.matriz_verdade[ID[0]]['data_min_max'] = grupo_cost.Data.min(), grupo_cost.Data.max()
            self.matriz_verdade[ID[0]]['valores'] = grupo_cost.Valor.values
        return self.matriz_verdade

    def normalize_df(self, df):
        # Converter a coluna 'Data' para datetime com o formato '%Y-%m-%d'
        if len(df['Data'].astype(str).str.slice(start=0, stop=10).values[0].split("-")[0]) == 4:
            df['Data'] = pd.to_datetime(df['Data'].astype(str).str.slice(start=0, stop=10), format='%Y-%m-%d',
                                        errors='coerce', infer_datetime_format=True)
        else:
            df['Data'] = pd.to_datetime(df['Data'].astype(str).str.slice(start=0, stop=10), format='%d-%m-%Y',
                                        errors='coerce', infer_datetime_format=True)
        # Converter a coluna 'Valor' para float
        df['Valor'] = pd.to_numeric(df['Valor'], errors='coerce', downcast='float')
        df = df.dropna(subset=['Referencia'])
        df = df.astype({'Referencia': 'str', 'Data': 'datetime64[ns]', 'Valor': 'float32'})
        df = df[['Referencia', 'Data', 'Valor']]
        return df

    def possibilities_matrix(self, df: pd.DataFrame):
        df = df.dropna()
        dataframe_costdriver = self.dataframe_costdriver.copy()

        df = self.normalize_df(df)
        df = df.groupby(['Data', 'Referencia'], as_index=False).mean()
        df = df.query("Data >= @dataframe_costdriver.Data.min() and Data <= @dataframe_costdriver.Data.max()")

        dataframe_costdriver = dataframe_costdriver.query("Data >= @df.Data.min() and Data <= @df.Data.max()")
        self.matriz_verdade = self.truth_matrix(dataframe_costdriver)

        def resample(sub_df):
            sub_df = sub_df.drop_duplicates(subset=['Data'])
            sub_df.index = sub_df.Data
            sub_df = sub_df.resample('M').ffill()
            sub_df['Data'] = sub_df.index
            sub_df = sub_df.reset_index(drop=True)
            return sub_df

        dfs = list()
        print("starting resample")
        for ref in df.Referencia.unique():
            sub_df = df.query("Referencia == @ref")
            sub_df = resample(sub_df)
            dfs.append(sub_df)

        print("resample finish")
        df = pd.concat(dfs)
        df.Data = df.Data.dt.strftime('01-%m-%Y')
        df['Data'] = pd.to_datetime(df['Data'], format="%d-%m-%Y", errors='coerce')

        df.sort_values(['Referencia', 'Data'], ascending=[True, True], inplace=True)

        """           
            pegar os indices que tem para todas as datas, pois é o que veio da plataforma
            para isso feito a intersecção de todas as datas.
        """

        self.referencia = ''
        for data, grupo in df.groupby(['Data']):
            if self.referencia == '':
                self.referencia = set(grupo.Referencia)
            else:
                set2 = set(grupo.Referencia)
                self.referencia = self.referencia.intersection(set2)

        self.referencia = np.array(list(self.referencia))
        self.datas = list()
        self.matrizes = list()

        for data, grupo in df.groupby(['Data']):
            grupo.index = grupo.Referencia
            grupo = grupo.loc[self.referencia]
            matriz = grupo['Valor'].values.reshape(-1, 1)
            self.matrizes.append(matriz)
            self.datas.append(data[0])

        self.matrizes = np.array(self.matrizes)
        self.matrizes = np.squeeze(self.matrizes)

    def compare(self):
        self.parametros = list()
        for ID in self.matriz_verdade.keys():
            matrix_test = self.matriz_verdade[ID]['valores']
            data_min_max = self.matriz_verdade[ID]['data_min_max']
            indices = [i for i, data in enumerate(self.datas) if data_min_max[0] <= data <= data_min_max[1]]
            matrix_test = np.tile(matrix_test.reshape(-1, 1), (1, self.matrizes.shape[1]))
            A = self.matrizes[indices, :] - matrix_test
            B = A.copy()
            A = abs(A.T)
            media_diferenças = np.mean(A, axis=1, keepdims=True)
            MAE = media_diferenças.min()
            indice = np.where(media_diferenças == MAE)[0]
            ref = self.referencia[indice]
            B = B / matrix_test
            B = abs(B.T)
            media_diferenças = np.mean(B, axis=1, keepdims=True)
            MAPE = media_diferenças.min() * 100
            self.parametros.append({"ID": ID, 'Referencia': ref, 'MAE': MAE, 'MAPE': MAPE})
        df_p = pd.DataFrame(self.parametros)
        return df_p


def free_list_proxy():
    pass
    # return [f'https://{ip}' for ip in pd.read_csv('Free_Proxy_List.csv')['ip'].to_list()]


def normalize(x): return unidecode(x.lower().replace('-', '').replace(' ', '').replace("(", "").replace(")", "").replace(' ', ''))


def normalize_text(text: str) -> str:
    text = unidecode(text)
    text = re.sub(r'[^a-zA-Z0-9]+', '', text)
    return text.strip().lower()


def get_token_costdrivers():
    # headers = {'key': config('ORIGIN_COSTDRIVER2'), 'email': config('USER_COSTDRIVER'),'pass': config('PASS_COSTDRIVER')}
    headers = {'key': '070F0E8A-E0C6-4970-8865-480650C0D12C',
               'email': 'datascience@datamark.com.br', 'pass': pass_word}
    req = {
        'url': f'{endpoint_cost}/api/v1/Auth',
        'method': 'GET'
    }
    print('req:\n', req)
    response = requests.get(req['url'], headers=headers)
    print('response.status_code: ', response.status_code)
    resp = response.json()['result']['tokenCode']
    print('__get_token_costdrivers: OK')
    return resp


def req_to_df(req):
    if not 'Error' in req.keys():
        df = pd.DataFrame(ast.literal_eval(req['result']['json']))
        if df.empty:
            data = {
                'ID': req['ID'],
                'Data': '-',
                'Valor': '-'
            }
            df = pd.DataFrame([data])
        df['success'] = req['success']
        df = df[['ID', 'Data', 'Valor', 'success']]
    else:
        df = pd.DataFrame([req])
        df['success'] = False
    return df


import asyncio
import aiohttp
import ssl
import certifi
import random

class ApiAsync:
    def __init__(self, jason: bool, lista: list, headers: dict = {
        'Accept': 'application/json',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36'
    }, proxy=None, workers=25):
        self.lista = lista
        self.headers = headers
        self.jason = jason
        self.proxy = proxy
        self.lista_proxy = None # Você precisará popular isso se for usar proxies
        self.semaphore = asyncio.Semaphore(workers)

    async def get_data(self, url: str, method: str, session: aiohttp.ClientSession, data: dict = None,
                       params: dict = None, json: dict = None, **kwargs):  # type: ignore
        method = method.lower()

        # Lógica de proxy simplificada para o exemplo
        proxy_url = None
        if self.proxy and self.lista_proxy:
            proxy_url = random.choice(self.lista_proxy)

        try:
            # Unifiquei a lógica de 'put' e outros métodos, pois o tratamento da resposta é o mesmo.
            # O método 'put' pode ser chamado via session.request('put', ...)
            async with session.request(method, url, data=data, params=params, json=json, proxy=proxy_url) as response:

                if response.status == 200:
                    if self.jason:
                        # Tenta decodificar a resposta como JSON
                        resultado = await response.json(content_type=None) # content_type=None ajuda com APIs que não retornam o header correto

                        # --- INÍCIO DA LÓGICA CORRIGIDA ---

                        # 1. Verifica se o resultado é uma lista e se tem o formato [cabeçalhos, ...dados]
                        if isinstance(resultado, list) and len(resultado) > 1 and all(isinstance(i, list) for i in resultado):
                            headers_list = resultado[0]
                            data_rows = resultado[1:]

                            # Converte a lista de listas em uma lista de dicionários
                            dados_formatados = [dict(zip(headers_list, row)) for row in data_rows]

                            # Adiciona os kwargs a cada dicionário na lista resultante
                            return [{**item, **kwargs} for item in dados_formatados]

                        # 2. Se o resultado já for uma lista de dicionários (ou similar)
                        elif isinstance(resultado, list):
                            return [{**x, **kwargs} for x in resultado]

                        # 3. Se o resultado for um único dicionário
                        elif isinstance(resultado, dict):
                            return {**resultado, **kwargs}

                        # 4. Caso seja um JSON válido mas em um formato inesperado, retorna como está
                        else:
                            return {'data': resultado, **kwargs}

                        # --- FIM DA LÓGICA CORRIGIDA ---

                    else: # Se self.jason for False
                        response_text = await response.text()
                        return {'html': response_text, **kwargs}

                # Se o status não for 200, retorna o erro
                return {"Error": response.status, "response": await response.text(), **kwargs}

        except aiohttp.ClientError as e:
            # Captura erros específicos do aiohttp (conexão, etc.)
            return {"Error": 'ClientError', "response": str(e), **kwargs}
        except Exception as e:
            # Captura outras exceções, como a TypeError original
            return {"Error": 'Exception', "response": e, **kwargs}

    async def _create_task(self):
        """
        It creates a task for each item in the list, and then gathers the results of all the tasks
        :return: A list of tasks.
        """
        # A configuração do conector SSL é uma ótima prática de segurança
        ssl_context = ssl.create_default_context(cafile=certifi.where())
        conn = aiohttp.TCPConnector(ssl=ssl_context)

        async with aiohttp.ClientSession(connector=conn, headers=self.headers) as session:
            tasks = []
            for item in self.lista:
                # O semáforo controla a concorrência para não sobrecarregar o servidor
                async with self.semaphore:
                    tasks.append(asyncio.ensure_future(self.get_data(
                        **item, session=session)))

            responses = await asyncio.gather(*tasks)
            return responses

    def run(self):
        """
        It creates a task that runs the function passed to it, and returns the result of that function
        :return: The return value of the coroutine object created by async def.
        """
        return asyncio.run(self._create_task())

class BotValidation:
    def __init__(self, df: pd.DataFrame, semestral=False, bot_name=None, df_excel=None, n_months=6,
                 tolerancia=0.01) -> None:
        self.tolerancia = tolerancia  #porcentagem de tolerancia entre valor coletado e valor da plataforma
        self.token_costdriver = get_token_costdrivers()
        self.df = df
        self.semestral = semestral
        self.date_now = datetime.now()
        self.bot_name = bot_name
        self.df_excel = df_excel
        self.n_months = n_months
        self.IDs_platform_date = 0
        self.url_gabarito = ''
        self.url_date_error = ''
        if self.df.shape[0] == 0:
            raise ValueError(f'self.df que entrou na validação está vazio!')

    def __make_lista_requisicao(self):
        lista_ids = self.df['ID'].unique()
        data_max = self.date_now
        days = 365
        if self.semestral:
            days += self.n_months * 31
        data_min = data_max - timedelta(days)
        data_max = data_max.strftime('%d-%m-%Y')
        data_min = data_min.strftime('%d-%m-%Y')
        total_ids = len(lista_ids)
        lista_requisicao = []
        for i in range(0, total_ids, 49):
            dict_ = {
                'url': f'{endpoint_cost}/api/v1/DataScience/option/11', 'method': 'get'}
            if (total_ids - i) < 49:
                dict_['params'] = {'ids': ','.join(list(map(
                    str, lista_ids[i:]))), 'dataCalculo1': data_min, 'dataCalculo2': data_max}  # type: ignore
            else:
                dict_['params'] = {'ids': ','.join(list(map(
                    str, lista_ids[i:i + 49]))), 'dataCalculo1': data_min, 'dataCalculo2': data_max}  # type: ignore
            lista_requisicao.append(dict_)
        print('__make_lista_requisicao: OK')
        self.data_min = data_min
        return lista_requisicao

    def __get_dados_costdrivers(self):
        lista_requisicao = self.__make_lista_requisicao()
        headers = {'Authorization': 'Bearer ' + self.token_costdriver}
        dados_costdrivers = ApiAsync(True, lista_requisicao, headers=headers).run()
        lista_dataframe = [pd.DataFrame(rsp['result'])
                           for rsp in dados_costdrivers if (not 'Error' in rsp.keys())]
        dataframe_costdriver = pd.concat(lista_dataframe, ignore_index=True)
        dataframe_costdriver = dataframe_costdriver.query("base == 0")
        dataframe_costdriver = dataframe_costdriver[['indicePrincipalID', 'dataIndice', 'valor']]
        dataframe_costdriver.columns = ['ID', 'Data', 'Valor']
        dataframe_costdriver['Data'] = pd.to_datetime(dataframe_costdriver['Data'], format='%Y-%m-%dT%H:%M:%S')
        print('__get_dados_costdrivers: OK')
        return dataframe_costdriver

    def __resample(self, sub_df):
        sub_df = sub_df.drop_duplicates(subset=['Data'])
        sub_df.index = sub_df.Data
        sub_df = sub_df.resample('M').ffill()
        sub_df['Data'] = sub_df.index
        sub_df = sub_df.reset_index(drop=True)
        return sub_df

    def __any_error(self, erros):
        erros_platform = [erros[key]['df'] for key in erros.keys() if
                          erros[key]['error'] != 'Platform max data higher than colected!']
        if len(erros_platform) > 0:
            # trello = AutoTrello(board="Gabarito")
            df = pd.concat(erros_platform, ignore_index=True)
            # trello.add_trello_validate(self.bot_name, df, df_excel=self.df_excel)
            _, self.url_gabarito = upload_bucket(bot_name=self.bot_name).upload_parquet(df, "gabarito")
        if self.IDs_platform_date > 0:
            erros_date = [erros[key]['df'] for key in erros.keys() if
                          erros[key]['error'] == 'Platform max data higher than colected!']
            df_date = pd.concat(erros_date, ignore_index=True)
            _, self.url_date_error = upload_bucket(bot_name=self.bot_name).upload_parquet(df_date, "date_error")

    def compare_dataframes(self, cutting: int = 2, percentage: float = 0.8):
        print('start compare_dataframes:')
        statistics = dict()
        df = self.df
        df.Data = pd.to_datetime(df.Data, errors='coerce')
        erros = {}
        dataframe_costdriver = self.__get_dados_costdrivers()
        df = df.reset_index(drop=True)
        dataframe_costdriver = dataframe_costdriver.reset_index(drop=True)
        dataframe_costdriver['ID'] = dataframe_costdriver['ID'].astype('int64')
        df['ID'] = df['ID'].astype('int64')
        dataframe_comparacao = df.merge(dataframe_costdriver, on=['ID', 'Data'], how='left',
                                        suffixes=('_coletado', '_plataforma'))
        dataframe_comparacao = dataframe_comparacao.dropna()
        print(
            f"BotValidation.compare_dataframes in_df.ID: {df.ID.nunique()} dataframe_costdriver.ID: {dataframe_costdriver.ID.nunique()} dataframe_comparacao.ID: {dataframe_comparacao.ID.nunique()}")
        df = df[df['ID'].isin(dataframe_costdriver.ID.unique())]
        dfn = df[~df['ID'].isin(dataframe_costdriver.ID.unique())]
        if dfn.shape[0] > 0:
            statistics['fora_plataforma'] = dfn.ID.unique()
        else:
            statistics['fora_plataforma'] = []
        df_dif_data = df[~df['ID'].isin(dataframe_comparacao.ID.unique())]
        if df_dif_data.shape[0] > 0:
            statistics['data_divergente'] = df_dif_data.ID.unique()
        else:
            statistics['data_divergente'] = []
        if self.semestral:
            cutting = self.n_months
            cutting_date = df.Data.max() - timedelta(days=cutting * 31)
        else:
            cutting_date = df.Data.max() - timedelta(days=cutting * 31)
        dataframe_comparacao = dataframe_comparacao.query(f"Data < '{cutting_date}'")
        if dataframe_comparacao.shape[0] == 0:
            raise ValueError(f'dataframe_comparacao has no data to compare!')
        dataframe_comparacao['Validacao'] = dataframe_comparacao['Valor_coletado'].astype(float) - dataframe_comparacao[
            'Valor_plataforma'].astype(float)
        dataframe_comparacao['Validacao'] = dataframe_comparacao['Validacao'] / dataframe_comparacao[
            'Valor_plataforma'].astype(float)
        dataframe_comparacao['Validacao'] = dataframe_comparacao['Validacao'].abs() <= self.tolerancia
        _, self.url_comparacao = upload_bucket(bot_name=self.bot_name).upload_parquet(dataframe_comparacao,
                                                                                      "comparacao")
        ids_validados = dataframe_comparacao[dataframe_comparacao['Validacao']].ID.unique()
        ids_erro = dataframe_comparacao[~dataframe_comparacao['Validacao']].ID.unique()
        ids_validados = [ID for ID in ids_validados if not ID in ids_erro]
        dfs_validados = []
        statistics['>=80'] = list()
        statistics['0<x<80'] = list()
        statistics['0'] = list()
        for ID in df.ID.unique():
            sub_df = df.query(f"ID == {ID}")
            sub_df = self.__resample(sub_df)
            colected_max_date = sub_df.Data.max()
            platform_max_date = dataframe_costdriver.query(f"ID == {ID}").Data.max()
            if self.semestral:
                n_months = self.n_months
                date_now = self.date_now
                date_aj = date_now - timedelta(31 * n_months)
                # 
                sub_df = sub_df.query(f"Data >='{date_aj.strftime('%Y-%m-01')}'")
                dfs_validados.append(sub_df)
            else:
                if (colected_max_date - platform_max_date).days > 31:
                    last_data = dataframe_costdriver.query(
                        f"ID == {ID} and Data == '{platform_max_date.strftime('%Y-%m-%d')}'")
                    if not last_data.Data.values[0] in sub_df.Data.unique():
                        sub_df = pd.concat(
                            [last_data, sub_df], ignore_index=True)
                    sub_df = self.__resample(sub_df)
                if ID in ids_validados:
                    sub_dfa = sub_df.query(f"Data >='{platform_max_date.strftime('%Y-%m-%d')}'")
                    if sub_dfa.empty:
                        erros[ID] = {}
                        erros[ID]['df'] = sub_df
                        erros[ID]['dfc'] = dataframe_costdriver.query(f"ID == {ID}")
                        erros[ID][
                            'error'] = f'Platform max data higher than colected!'
                        dfs_validados.append(sub_df)
                        self.IDs_platform_date += 1
                        print("Platform max data higher than colected! ", ID)
                    else:
                        dfs_validados.append(sub_dfa)
                if ID in ids_erro:
                    # ''' modificar processo do historico para validar uma janela de n meses com m meses de descarte e x% de acerto'''
                    df_c = dataframe_comparacao.query(f"ID == {ID}").copy()
                    df_c['process_at'] = datetime.now()
                    acertos = df_c.Validacao.sum() / df_c.shape[0]
                    if acertos >= percentage:
                        sub_dfa = sub_df.query(f"Data >='{platform_max_date.strftime('%Y-%m-%d')}'")
                        if sub_dfa.empty:
                            erros[ID] = {}
                            erros[ID]['df'] = sub_df
                            erros[ID]['dfc'] = dataframe_costdriver.query(f"ID == {ID}")
                            erros[ID][
                                'error'] = f'Platform max data higher than colected!'
                            dfs_validados.append(sub_df)
                            print("Platform max data higher than colected! ", ID)
                        else:
                            dfs_validados.append(sub_dfa)
                    else:
                        erros[ID] = {}
                        erros[ID]['df'] = df_c
                        erros[ID][
                            'error'] = f'Colected data score is lower than defined: {percentage}'
                        if acertos <= 0:
                            statistics['0'].append(ID)
                        else:
                            statistics['0<x<80'].append(ID)
        self.__any_error(erros=erros)
        if len(dfs_validados) > 0:
            df_val = pd.concat(dfs_validados, ignore_index=True)
            statistics['>=80'] = list(df_val.ID.unique())
            df_val.sort_values(['ID', 'Data'], ascending=[
                True, True], inplace=True)
            df_val['Data'] = df_val['Data'].dt.strftime('01-%m-%Y')
        else:
            statistics['>=80'] = []
            df_val = pd.DataFrame(columns=['ID', 'Data', 'Valor'])
        self.statistics = statistics
        self.erros = erros
        if df_val.shape[0] > 0:
            return df_val
        else:
            print(f'DataFrame vazio\nAll Data has score lower than defined: {percentage}')
            return pd.DataFrame(columns=['ID', 'Data', 'Valor'])


class upload_bucket:

    def __init__(self, my_bucket="mwaa-collected-data", bot_name=None) -> None:
        self.current_folder = datetime.now().strftime("%Y-%m-%d")
        self.AzureBlob = AzureBlobStorage()
        self.region = 'eu-central-1'
        self.my_bucket = my_bucket
        self.bot_name = bot_name

    def ajust_parquet_final(self, df: pd.DataFrame):
        if 'success' in df.columns:
            df = df[['ID', 'Data', 'Valor', 'success', 'bot_name']]
        else:
            df = df[['ID', 'Data', 'Valor']]
        # Converter a coluna 'ID' para float
        df['ID'] = pd.to_numeric(df['ID'], errors='coerce', downcast='integer')
        # Converter a coluna 'Data' para datetime com o formato '%Y-%m-%d'
        if len(df['Data'].astype(str).str.slice(start=0, stop=10).values[0].split("-")[0]) == 4:
            df['Data'] = pd.to_datetime(df['Data'].astype(str).str.slice(start=0, stop=10), format='%Y-%m-%d',
                                        errors='coerce', infer_datetime_format=True)
        else:
            df['Data'] = pd.to_datetime(df['Data'].astype(str).str.slice(start=0, stop=10), format='%d-%m-%Y',
                                        errors='coerce', infer_datetime_format=True)
        # Converter a coluna 'Valor' para float
        df['Valor'] = pd.to_numeric(df['Valor'], errors='coerce', downcast='float')
        df = df.dropna()
        df = df.astype({'ID': 'int32', 'Data': 'datetime64[ns]', 'Valor': 'float32'})
        return df

    def ajust_parquet(self, df: pd.DataFrame):
        print("df:\n", df)
        df = df.reset_index(drop=True)
        # na_cols = [col for col in df.columns if sum(df[col].isna()) == df.shape[0]]
        na_cols = [col for col in df.columns if df[col].empty]
        if len(na_cols) > 0:
            df = df.drop(columns=na_cols)
        if sum([type(i) == str for i in df.columns]) != df.shape[1]:
            df.columns = [f'col_{n}' for n in df.columns]
        if df.columns.drop_duplicates().shape[0] != df.shape[1]:
            df.columns = [f'{col}_{i}' for i, col in enumerate(df.columns)]
        df = df.fillna('')
        df = df.astype({col: 'str' for col in df.columns})
        return df

    def upload_csv(self, df, name):
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, sep='|')
        print("upload_bucket.upload_csv: ", name)

    def __up_bucket(self, name, df_i):
        parquet_buffer = BytesIO()
        df_i.to_parquet(parquet_buffer, engine='pyarrow', compression='gzip')
        self.AzureBlob.upload_azure(name=name, data=df_i, extension='.parquet')
        print('upload_bucket.upload_parquet: ', name)

    def upload_json(self, name, dicio):
        json_string = json.dumps(dicio)
        self.AzureBlob.upload_azure(name=name, data=json_string, extension='.json')
        print('upload_bucket.upload_json: ', name)

    def upload_parquet(self, df, TAG, df_t=pd.DataFrame()):
        df_i = df.copy()
        if df_i.index.__class__ is pd.MultiIndex:
            df_i = df_i.reset_index(level=df_i.index.names)
        if df_i.columns.__class__ is pd.MultiIndex:
            df_i.columns = [f'{level1}_{level2}' for (level1, level2) in df_i.columns]
        if df_i.columns.isin(['ID', 'Data', 'Valor']).sum() == 3:
            df_i = self.ajust_parquet_final(df_i)
        else:
            df_i = self.ajust_parquet(df_i)
        name = TAG + '/' + self.current_folder + '/' + datetime.now().strftime(
            "%H:%M%S") + '_' + self.bot_name + '.parquet'
        self.__up_bucket(name=name, df_i=df_i)
        if (TAG != 'coletado') and (TAG != 'faltante_cadastro') and (TAG != 'error_upload'):
            name_traking = 'traking/' + self.bot_name + '.parquet'
            df_i['TAG'] = TAG
            if df_t.shape[0] > 0:
                df_t['ID'] = df_t['ID'].astype(int)
                df_i['ID'] = df_i['ID'].astype(int)
                df_i = pd.concat([df_t[~df_t.ID.isin(df_i.ID.unique())], df_i])
            df_i = df_i.groupby(['ID', 'TAG'], as_index=False).max()
            df_i['bot_name'] = self.bot_name
            df_i['process_at'] = datetime.now()
            df_i['where'] = 'AIRFLOW' if "/opt/airflow/" in str(os.getcwd()) else "NOT_AIRFLOW"
            df_i['data_at'] = name
            df_i = df_i[['ID', 'bot_name', 'TAG', 'process_at', 'where']]
            df_i = df_i.astype({'ID': int, 'bot_name': str, 'TAG': str, 'process_at': 'datetime64[ns]', 'where': str})
            self.__up_bucket(name=name_traking, df_i=df_i.rename(columns={'TAG': 'etapa'}))
        return df_i, name


class bucket_manager(upload_bucket):
    def __init__(self):
        super().__init__()

    def get_bucket_file(self, S3_URI):
        my_bucket = S3_URI.split("//")[-1].split("/")[0]
        file = '/'.join(S3_URI.split("//")[-1].split("/")[1:])
        return self.AzureBlob.download_parquet(my_bucket, file)

class UploadData:
    '''
    strp_manager_manager_32
    '''

    def __init__(self, df: pd.DataFrame, bot_name, token_costdriver, out=60, ExcluirHistorico='N') -> None:
        self.out = out  #4 para bypass
        self.bot_name = bot_name
        self.token_costdriver = token_costdriver
        self.ExcluirHistorico = ExcluirHistorico
        self.df = self.normalize_types(df) if df.shape[0] > 0 else pd.DataFrame()
        self.lista_requisicao = self.__requisition_list() if self.df.shape[0] > 0 else []

    def normalize_types(self, df):
        if type(df.ID.values[0]) != int:
            df.ID = df.ID.astype(int)
        if not 'datetime' in type(df.Data.values[0]).__name__:
            df.Data = pd.to_datetime(df.Data, format="%d-%m-%Y")
        if df.shape[0] > 0:
            if type(df.Valor.values[0]) != float:
                df.Valor = df.Valor.astype(float)
            df.sort_values(['ID', 'Data'], ascending=[True, True], inplace=True)
            return df
        else:
            return pd.DataFrame()

    def __requisition_list(self):
        df = self.df.copy()
        date_now = datetime.now()
        date_now = date(date_now.year, date_now.month, 1)
        ids = df.ID.unique()
        lista_requisicao = []
        for ID in ids:
            rjson = {}
            json_ = df.query(f"ID == {ID}").copy()
            # json_['Base'] = ['0' if c else '1' for c in json_['Data'] < datetime.now().strftime('%Y-%m-01')]
            json_['Base'] = '0'  #added for ticket #2733639206
            json_.Data = json_.Data.dt.strftime("01-%m-%Y")
            if '1' in json_['Base'].unique():
                print(f"ID: {ID} has forecasted data")
            json_['Explicativa'] = '0'
            rjson["opc"] = 9
            rjson["idioma"] = "1"
            rjson["identity"] = '2258FB7E-7F19-483E-BBAE-8250973D3658'
            rjson["percentualOutlier"] = self.out
            rjson["merchantID"] = ""
            rjson['json'] = str(json_.to_dict(
                orient='records')).replace("'", '"')
            rjson['jsonSeq'] = str({'ID': str(
                ID), 'ExcluirHistorico': self.ExcluirHistorico, 'Origem': 'Data Science'}).replace("'", '"')
            lista_requisicao.append(
                {'url': f'{endpoint_cost}/api/v1/DataScience/UpdateOption-9', 'method': 'put',
                 'data': rjson, 'ID': ID})
        return lista_requisicao

    def recursively_upload(self, lista_requisicao, headers, resp=[]):
        print('lista_requisicao lista_requisicao len is: ', len(lista_requisicao))
        upload_data = ApiAsync(True, lista_requisicao, headers=headers).run()
        nones_index = [i for i, val in enumerate(upload_data) if val == None]
        if len(nones_index) == len(lista_requisicao):
            print("Não fez mais nenhuma requisição!")
            return resp
        if len(nones_index) > 0:
            lista_requisicao = [val for i, val in enumerate(
                lista_requisicao) if i in nones_index]
            print('Starting recursion.. new lista_requisicao is: ',
                  len(lista_requisicao))
            if len(resp) > 0:
                resp += [val for i,
                val in enumerate(upload_data) if i not in nones_index]
            else:
                resp = [val for i, val in enumerate(
                    upload_data) if i not in nones_index]
            return self.recursively_upload(lista_requisicao, headers, resp)
        else:
            if len(resp) == 0:
                resp = upload_data
            return resp

    def upload_data(self):
        lista_requisicao = self.lista_requisicao
        qtd_ids = len(lista_requisicao)

        if qtd_ids > 0:
            headers = {
                "accept": "*/*", 
                "Content-Type": "application/json-patch+json",
                'Authorization': 'Bearer ' + self.token_costdriver
            }
            
            chunk_size = 15000
            dfs = []
            
            # Loop dividindo a lista em chunks de 15000
            for i in range(0, qtd_ids, chunk_size):
                lista_chunk = lista_requisicao[i:i + chunk_size]
                upload_data_chunk = self.recursively_upload(lista_chunk, headers)
                dfs_chunk = [req_to_df(req) for req in upload_data_chunk]
                dfs.extend(dfs_chunk)
            
            self.df_uploaded = pd.concat(dfs, ignore_index=True)
            self.df_uploaded['bot_name'] = self.bot_name
        else:
            self.df_uploaded = pd.DataFrame()

    def conferencia(self):
        pass


class AutoTrello:
    def __init__(self, board="Stack") -> None:
        self.board = board
        self.__init()

    def __init(self):
        self.idListToDo = []
        print("Starting AutoTrello for: ", self.board)

        if ('airflow' in os.getcwd().lower()) and (self.board != 'Stack'):
            self.idBoard_ = 'Validacoes'
            self.idBoard = self.TrelloGetBoardID(self.idBoard_)
            self.idListProgress = self.TrelloGetListID(self.idBoard, listname='Progress')
            self.idListDE = self.TrelloGetListID(self.idBoard, listname='Data Engineer')
        else:
            self.idBoard_ = 'Stack Tecnologias'
            self.idBoard = self.TrelloGetBoardID(self.idBoard_)
            self.idListProgress = self.TrelloGetListID(self.idBoard, listname='Validações - AutoTrello')

        if (self.board == "Stack"):
            self.idListToDo = self.TrelloGetListID(self.idBoard, listname='Validações - AutoTrello')
            self.checklistitems = ['Diagnóstico', 'Atribuir responsável',
                                   'Iniciar Manutenção', 'Code Review', 'Update']
        elif self.board == "Gabarito":
            listname = 'Gabarito (divergencia coleta/historico)' if 'airflow' in os.getcwd().lower() else 'Validações - AutoTrello'
            self.idListToDo = self.TrelloGetListID(self.idBoard, listname=listname)
            self.checklistitems = ['Diagnóstico', 'Atribuir responsável',
                                   'Iniciar revisão', 'Update/Change historico']
        elif self.board == "Cadastro":
            listname = 'Cadastro (tem no sistema e nao tem planilha)' if 'airflow' in os.getcwd().lower() else 'Validações - AutoTrello'
            self.idListToDo = self.TrelloGetListID(self.idBoard, listname=listname)
            self.checklistitems = ['Diagnóstico', 'Atribuir responsável',
                                   'Iniciar revisão', 'Enviar nova planilha/Exclusão ID']
        elif self.board == "Desatualizado":
            listname = 'Desatualizado (valor coletado menor do que distancia definida)' if 'airflow' in os.getcwd().lower() else 'Validações - AutoTrello'
            self.idListToDo = self.TrelloGetListID(self.idBoard, listname=listname)
            self.checklistitems = ['Diagnóstico', 'Atribuir responsável',
                                   'Validar se fonte mudou', 'Nova documentação/Exclusão ID']
        elif self.board == "Mudança":
            listname = 'Mudança indicador (algum ID não foi pego)' if 'airflow' in os.getcwd().lower() else 'Validações - AutoTrello'
            self.idListToDo = self.TrelloGetListID(self.idBoard, listname=listname)
            self.checklistitems = ['Diagnóstico', 'Atribuir responsável',
                                   'Validar se mudou nome na fonte', 'Nova documentação/Exclusão ID']
        elif self.board == "Modificada":
            listname = 'Fonte modificada' if 'airflow' in os.getcwd().lower() else 'Validações - AutoTrello'
            self.idListToDo = self.TrelloGetListID(self.idBoard, listname=listname)
            self.checklistitems = ['Diagnóstico', 'Atribuir responsável',
                                   'Validar se mudou a fonte/documento de onde pegavamos', 'Nova documentação']

        if ('airflow' in os.getcwd().lower()) and (self.idBoard_ == 'Validacoes'):
            if len(self.idListToDo) > 0:
                self.idLists = [self.idListProgress, self.idListToDo, self.idListDE]
            else:
                self.idLists = [self.idListDE]
        else:
            self.idLists = [self.idListProgress, self.idListToDo]
        self.CardsOnBoard = self.TrelloGetsCardOnBoardLists(self.idBoard, self.idLists)

    def __CDquery(self, query={}):
        query['key'] = 'removed'
        query['token'] = 'removed'
        return query

    def __TrelloGetsPost(self, url, query={}, mode='GET'):
        headers = {
            "Accept": "application/json"
        }
        response = requests.request(
            mode,
            url,
            headers=headers,
            params=self.__CDquery(query)
        )
        if response.status_code == 200:
            return response.json()
        else:
            raise IOError('error {}: {}'.format(
                response.status_code, response.text))

    def TrelloGetBoardID(self, BoardName):
        url = "https://api.trello.com/1/members/me/boards"
        response = self.__TrelloGetsPost(url)
        BoardId = [r['id'] for r in response if BoardName in r['name']][0]
        return BoardId

    def TrelloGetListID(self, idBoard, listname='To Do'):
        url = f"https://api.trello.com/1/boards/{idBoard}/lists"
        response = self.__TrelloGetsPost(url)
        idlist = [idList['id'] for idList in response if listname in idList['name']]
        if len(idlist) > 0:
            idlist = idlist[0]
        return idlist

    def TrelloCreateCardChecklist(self, query, checklistitems):
        url = "https://api.trello.com/1/cards"
        response = self.__TrelloGetsPost(url, query, 'POST')
        url = "https://api.trello.com/1/cards/{}/checklists".format(
            response['id'])
        checklist = self.__TrelloGetsPost(url, mode='POST')
        url = "https://api.trello.com/1/checklists/{}/checkItems".format(
            checklist['id'])
        for checklistitem in checklistitems:
            query = {'name': checklistitem}
            checklist = self.__TrelloGetsPost(url, query, mode='POST')
        return response

    def TrelloCreateCardChecklistAttachment(self, query, checklistitems, filename, excel_name, df_excel):
        url = "https://api.trello.com/1/cards"
        response = self.__TrelloGetsPost(url, query, 'POST')
        url = "https://api.trello.com/1/cards/{}/checklists".format(
            response['id'])
        checklist = self.__TrelloGetsPost(url, mode='POST')
        url = "https://api.trello.com/1/checklists/{}/checkItems".format(
            checklist['id'])
        for checklistitem in checklistitems:
            query = {'name': checklistitem}
            checklist = self.__TrelloGetsPost(url, query, mode='POST')
        url = "https://api.trello.com/1/cards/{}/attachments".format(
            response['id'])
        with open(filename, 'rb') as arquivo:
            files = {'file': arquivo}
            requests.post(url, params=self.__CDquery(), files=files)
            print("TrelloCreateCardChecklistAttachment filename: ", filename)
        if df_excel.shape[0] > 0:
            if 'airflow' in os.getcwd():
                excel_name = os.path.join('/usr/local/airflow/dags/', excel_name)
                with open(excel_name, 'rb') as arquivo:
                    files = {'file': arquivo}
                    r = requests.post(url, params=self.__CDquery(), files=files)
                    print('r is: ', r.ok)
        return response

    def TrelloGetsCardOnBoardLists(self, idBoard, idLists, expand=False):
        url = f"https://api.trello.com/1/boards/{idBoard}/cards"
        response = self.__TrelloGetsPost(url)
        if expand:
            CardsOnBoard = [i for i in response if i['idList'] in idLists]
        else:
            CardsOnBoard = [i['name'] for i in response if i['idList'] in idLists]
        return CardsOnBoard

    def add_trello_task(self, botname, task_error, error):
        if botname not in self.CardsOnBoard:
            query = {
                'name': f'{botname} - AutoTrello',
                'desc': f'Task_error: {task_error}\nError is:\n{error}',
                'idList': self.idListToDo
            }
            self.TrelloCreateCardChecklist(query, self.checklistitems)
            print('add_trello_task: Criada task no Trello!')
        else:
            print('add_trello_task: Erro ja cadastrado!')

    def add_trello_validate(self, botname, df=pd.DataFrame(), df_excel=pd.DataFrame()):
        name = f'{botname} - {self.board}'
        if self.board == 'Gabarito':
            desc = f'Os indicadores no anexo estão com valores divergentes'
        elif self.board == 'Cadastro':
            desc = f'Os indicadores no anexo estão associados a(s) fonte(s) mas não estão na planilha do bot, favor revisar.'
        elif self.board == 'Desatualizado':
            desc = f'Os indicadores em anexo estão parados nas respectivas datas de coleta, favor revisar se houve mudança na fonte.'
        elif self.board == 'Mudança':
            desc = f'Os indicadores em anexo não foram encontrados na fonte, favor verificar se houve alteração no nome do indicador.'
        elif self.board == 'Modificada':
            desc = f'O endereço passado não está funcionando/Documento encontrado se encontra vazio.'
        if (name not in self.CardsOnBoard):
            query = {
                'name': name,
                'idList': self.idListToDo
            }
            query['desc'] = desc
            if df.shape[0] > 0:
                filename = f'{self.board}_{botname}.xlsx'
                # df.to_excel(filename)  # Comentado - não salvar Excel localmente
                excel_name = f'{botname}.xlsx'
                print("self.idBoard_: ", self.idBoard_)
                if (self.idBoard_ != 'Stack Tecnologias'):
                    self.TrelloCreateCardChecklistAttachment(query, self.checklistitems, filename, excel_name, df_excel)
                    os.remove(filename)
            else:
                if self.board == 'Gabarito':
                    query['desc'] = f'Nao possuimos IDs com historico na plataforma!'
                if (self.idBoard_ != 'Stack Tecnologias'):
                    self.TrelloCreateCardChecklist(query, self.checklistitems)
            print('add_trello_validate: Criada task no Trello!')
        else:
            print('add_trello_validate: Erro ja cadastrado!')


def TrelloDecorator(func):
    def wrapper(self, *args, **kwargs):
        try:
            return func(self, *args, **kwargs)
        except BaseException as error:
            raise error

    return wrapper


def LogErrorDecorator(func):
    def wrapper(self, *args, **kwargs):
        try:
            return func(self, *args, **kwargs)
        except BaseException as error:
            error = str(type(error)).split(
                ' ')[-1].replace("'", '').replace('>', '') + ' ' + str(error)
            errorType = error.split(" ")[0]
            error = ' '.join(error.split(" ")[1:])
            func_name = func.__name__
            tempo_execucao = datetime.now().timestamp() - self.init_timestamp.timestamp()
            data = self.init_timestamp.strftime("%d-%m-%YT%H:%M:%S")
            local = self.get_execution_local()
            log_error = {
                "bot_name": self.bot_name,
                "data": data,
                "tempo_execucao": tempo_execucao,
                "etapa_wrapper": self.last_step,
                "metodo_classe": func_name,
                "errorType": errorType,
                "error": error,
                "local": local,
            }
            folder_data = self.init_timestamp.strftime("%Y-%m-%d")
            upload_bucket(bot_name=self.bot_name).upload_json(
                name=f"error_logs/{folder_data}/{data}_{self.bot_name}.json", dicio=log_error)
            raise

    return wrapper


def for_all_methods(decorator):
    def decorate(cls):
        for attr in cls.__dict__:
            if callable(getattr(cls, attr)):
                setattr(cls, attr, decorator(getattr(cls, attr)))
        return cls

    return decorate


# @for_all_methods(FailDecorator)
class findFailure:
    def __init__(self, bot_name, path_dir=os.path.dirname(os.path.abspath(__file__))) -> None:
        self.bot_name = bot_name
        self.path_dir = path_dir
        self.data_atual = pd.to_datetime("today")

    def ids_fonte(self, df_ids: pd.DataFrame):
        bucket = bucket_manager()
        print("init ids_fonte:\n", df_ids)
        df_ids.ID = df_ids.ID.astype(str)
        self.df_ids = df_ids
        filename = "fonte_id_atualizacao.parquet"
        filename = os.path.join(self.path_dir, filename)
        filename1 = "concat_tratados.parquet"
        filename1 = os.path.join(self.path_dir, filename1)
        df = pd.read_parquet(filename, engine='pyarrow')
        self.df_parquet = df.copy()
        df1 = pd.read_parquet(filename1, engine='pyarrow')
        df1['ID'] = df1['ID'].astype(str)
        df['id'] = df['id'].astype(str)
        df_ids['ID'] = pd.to_numeric(df_ids['ID'], errors='coerce')
        df_ids.dropna(subset=['ID'], inplace=True)
        df_ids.ID = df_ids.ID.astype(str)
        self.df_excel = df_ids.copy()
        df_nao = df_ids[df_ids['ID'].isin(df.loc[~df['dataMax_Real'].isna(), 'id'].unique())]
        if not df_nao.empty:
            self.df_falt_hist = df_nao
            print("----------- temos dados novos que precisão de historico --------------------\n", df_nao)
        fontes = df[df['id'].isin(df_ids.ID.unique())].fonte.unique()
        df = df[df['fonte'].isin(fontes)]
        df.dataMax_Real = pd.to_datetime(df.dataMax_Real, format="%Y-%m-%d")
        self.parquet = df.copy()
        self.parquet = self.parquet.rename(columns={"id": "ID"})
        return self.parquet
        # df["dif"] = ((self.data_atual - df["dataMax_Real"]) / 31)
        # df["dif"] = df["dif"].dropna().astype(int)
        # if df["dif"].sum() == 0:
        #     return self.parquet
        # else:
        #     df_faltantes = df[~df['id'].isin(df_ids.ID.unique())]
        #     df_faltantes['status'] = 'faltante'
        #     df_faltantes = df_faltantes.rename(columns={"id": "ID"})
        #     other_bot = df_faltantes.loc[df_faltantes['ID'].isin(df1.ID.unique())]
        #     if not other_bot.empty:
        #         df_faltantes.loc[df_faltantes['ID'].isin(df1.ID.unique()), 'status'] = \
        #         df1.loc[df1['ID'].isin(other_bot.ID.unique())]['bot_name'].values
        #     if df_faltantes.shape[0] > 0:
        #         df_faltantes = df_faltantes.sort_values(['ID', 'fonte', 'status'], ascending=[True, True, True])
        #         # AutoTrello(board='Cadastro').add_trello_validate(botname=self.bot_name, df=df_faltantes, df_excel=self.df_excel)
        #         df_faltantes = df_faltantes.query("status == 'faltante'")
        #         if not df_faltantes.empty:
        #             df_faltantes = df_faltantes[['ID', 'fonte', 'dataMax_Real']]
        #             upload_bucket(bot_name=self.bot_name).upload_parquet(df_faltantes, "faltante_cadastro")
        #     return self.parquet
    def fonte_atualizada(self, df: pd.DataFrame, threshold: int = 6):
        print("init fonte_atualizada")
        if len(df['Data'].astype(str).str.slice(start=0, stop=10).values[0].split("-")[0]) == 4:
            df['Data'] = pd.to_datetime(df['Data'].astype(str).str.slice(start=0, stop=10), format='%Y-%m-%d',
                                        errors='coerce', infer_datetime_format=True)
        else:
            df['Data'] = pd.to_datetime(df['Data'].astype(str).str.slice(start=0, stop=10), format='%d-%m-%Y',
                                        errors='coerce', infer_datetime_format=True)
        df_gb = df[['ID', 'Data']].groupby(['ID'], as_index=False).max()
        df_gb["dif"] = ((self.data_atual - df_gb["Data"]) / 31).astype(int)
        df_gb = df_gb[df_gb["dif"] > threshold]
        if df_gb.shape[0] > 0:
            df_gb = df_gb[['ID', 'Data']]
            # AutoTrello(board='Desatualizado').add_trello_validate(botname=self.bot_name, df=df_gb)
            if df_gb.shape[0] == df.shape[0]:
                raise ValueError("todos os dados estão desatualizados")

    def falha_merge(self, df: pd.DataFrame):
        print("init falha_merge")
        df_faltantes = self.df_ids[~self.df_ids['ID'].isin(df.ID.unique())]
        if df_faltantes.shape[0] > 0:
            df_faltantes = df_faltantes.merge(self.parquet, on=['ID'], how='left')
            # AutoTrello(board='Mudança').add_trello_validate(botname=self.bot_name, df=df_faltantes, df_excel=self.df_excel)
        return df_faltantes

    def fonte_modificada(self, error_list: list):
        print("init fonte_modificada")
        # df_erro = pd.DataFrame(error_list)
        # AutoTrello(board='Modificada').add_trello_validate(bot_name=self.bot_name, df=df_erro)
        pass


class DailyProcess:
    def __init__(self, df, bot_name, df_excel=pd.DataFrame(), out=60, ExcluirHistorico='N', tolerancia=0.01) -> None:
        self.tolerancia = tolerancia  #porcentagem de tolerancia entre valor coletado e valor da plataforma
        self.df = self.normalize_types(df)
        self.PercentualOutlier = out
        self.bot_name = bot_name
        self.email = 'datascience@datamark.com.br'
        self.passwd = pass_word
        self.origin_key = '070F0E8A-E0C6-4970-8865-480650C0D12C'
        self.date_now = datetime.now()
        self.token_now = get_token_costdrivers()
        self.get_token_old()
        self.df_excel = df_excel
        self.ExcluirHistorico = ExcluirHistorico
        self.headers_daily = {'Authorization': 'Bearer ' + self.token_old, 'Content-Type': 'application/json'}
        self.bypass_validation = False
        self.url_comparacao = None

    def normalize_types(self, df):
        if type(df.ID.values[0]) != str:
            df.ID = df.ID.astype(int)
            df.ID = df.ID.astype(str)
        if not 'datetime' in type(df.Data.values[0]).__name__:
            df.Data = pd.to_datetime(df.Data, format="%d-%m-%Y")
        if type(df.Valor.values[0]) != float:
            df.Valor = df.Valor.astype(float)
        df.sort_values(['ID', 'Data'], ascending=[True, True], inplace=True)
        return df

    def get_token_old(self):
        r = requests.post(
            'https://api.costdrivers.com/token',
            data=f'username={self.email}&password={self.passwd}&grant_type=password',
            headers={'Origem': self.origin_key}
        )
        self.token_old = r.json()['access_token']
        print('__get_old_token_costdrivers: OK')

    def create_validation_list(self, n_months=3):
        data_max = self.date_now
        days = 31 * n_months
        data_min = data_max - timedelta(days)
        data_max = data_max.strftime('%d-%m-%Y')
        data_min = data_min.strftime('%d-%m-%Y')
        params_ = {"Internal": "True", "Daily": "True", "IDs": "ID", 'StartDate': data_min, 'EndDate': data_max}
        self.lista_requisicao = []
        for ID in self.df.ID.unique():
            params = params_.copy()
            params["IDs"] = str(ID)
            self.lista_requisicao.append(
                {'url': 'https://api.costdrivers.com/api/Indicador', 'method': 'get', 'params': params})
        print('__create_validation_list: OK')

    def get_plataform_data(self):
        self.create_validation_list()
        dados_costdrivers = ApiAsync(True, self.lista_requisicao, headers=self.headers_daily).run()
        # 
        lista_dataframe = []
        for i, rsp in enumerate(dados_costdrivers):
            print('type(rsp): ', type(rsp))
            print('rsp: ', rsp)
            try:
                if isinstance(rsp, list):
                    df = pd.DataFrame(rsp)
                    lista_dataframe.append(df)
            except Exception as e:
                print(f"Erro ao processar resposta #{i}: {e}")
                lista_dataframe.append(pd.DataFrame())
        # 
        df_costdrivers = pd.concat(lista_dataframe, ignore_index=True)
        if not df_costdrivers.empty:
            df_costdrivers = df_costdrivers[['ID', 'Data', 'Valor']]
            df_costdrivers = self.normalize_types(df_costdrivers)
        else:
            df_costdrivers = pd.DataFrame(columns=['ID', 'Data', 'Valor'])
        self.df_costdrivers = df_costdrivers
        print('__get_plataform_data: OK')

    def validate(self, cutting: int = 3, percentage: float = 0.8):
        self.get_plataform_data()
        self.statistics = {}
        self.statistics['0'] = list()
        self.statistics['0<x<80'] = list()
        self.statistics['>=80'] = list()
        self.statistics['fora_plataforma'] = list()
        self.statistics['data_divergente'] = list()
        if self.df.Data.min() not in self.df_costdrivers.Data.unique():
            self.dfs_validados = self.df.copy()
            self.dfs_validados.ID = self.dfs_validados.ID.astype(str)
            self.bypass_validation = True
            self.dfs_validados.dropna(subset=['ID'], inplace=True)
            return print('validation bypassed')
        last_3_day = self.date_now - cutting * BDay()
        last_3_day = last_3_day.strftime('%Y-%m-%d')
        max_date_ID = self.df_costdrivers.groupby(['ID'], as_index=False).max()
        min_date_ID = max_date_ID.Data.min() - 20 * BDay()
        min_date_ID = min_date_ID.strftime('%Y-%m-%d')
        dataframe_comparacao = self.df_costdrivers.merge(self.df, on=["ID", "Data"],
                                                         suffixes=("_plataforma", "_coletado"), how='right')
        dataframe_comparacao.dropna(subset=['Valor_plataforma'], inplace=True)
        # dataframe_comparacao = dataframe_comparacao.query("Data > @min_date_ID and Data < @last_3_day")
        dataframe_comparacao = dataframe_comparacao.query("Data > @min_date_ID")
        dataframe_comparacao['Validacao'] = dataframe_comparacao['Valor_coletado'].astype(float) - dataframe_comparacao[
            'Valor_plataforma'].astype(float)
        dataframe_comparacao['Validacao'] = dataframe_comparacao['Validacao'] / dataframe_comparacao[
            'Valor_plataforma'].astype(float)
        dataframe_comparacao['Validacao'] = dataframe_comparacao['Validacao'] <= self.tolerancia
        dataframe_comparacao_val = dataframe_comparacao.copy()
        _, self.url_comparacao = upload_bucket(bot_name=self.bot_name).upload_parquet(dataframe_comparacao,
                                                                                      "comparacao")
        dfn = self.df[~self.df['ID'].isin(self.df_costdrivers.ID.unique())]
        if dfn.shape[0] > 0:
            self.statistics['fora_plataforma'] = dfn.ID.unique()
        df_dif_data = self.df[~self.df['ID'].isin(dataframe_comparacao_val.ID.unique())]
        df_dif_data = df_dif_data[~df_dif_data['ID'].isin(dfn.ID.unique())]
        if df_dif_data.shape[0] > 0:
            self.statistics['data_divergente'] = df_dif_data.ID.unique()
        print(
            f"DailyProcess.validate df.ID: {self.df.ID.nunique()} dataframe_costdriver.ID: {self.df_costdrivers.ID.nunique()} dataframe_comparacao.ID: {dataframe_comparacao_val.ID.nunique()}")
        if len(self.statistics['fora_plataforma']) > 0:
            print("faltantes ", self.statistics['fora_plataforma'])
        ids_validados = dataframe_comparacao_val[dataframe_comparacao_val['Validacao']].ID.unique()
        ids_erro = dataframe_comparacao_val[~dataframe_comparacao_val['Validacao']].ID.unique()
        ids_validados = [ID for ID in ids_validados if not ID in ids_erro]
        self.dfs_validados = [self.df.query("ID == @ID") for ID in ids_validados]
        erros = []
        for ID in ids_erro:
            sub_comparacao = dataframe_comparacao_val.query("ID == @ID")
            max_date = max_date_ID.loc[max_date_ID['ID'] == ID, 'Data'].dt.strftime("%Y-%m-01").values[0]
            acertos = (sub_comparacao.shape[0] - sub_comparacao.Validacao.value_counts()[False]) / sub_comparacao.shape[
                0]
            if acertos > percentage:
                self.dfs_validados.append(self.df.query("ID == @ID and Data >= @max_date"))
                self.statistics['>=80'].append(ID)
            else:
                sub_comparacao = dataframe_comparacao.query("ID == @ID")
                sub_comparacao = sub_comparacao.reset_index(drop=True)
                s = sub_comparacao.query("Data < @max_date")
                ind = s.loc[s['Valor_plataforma'].isna()]
                if len(ind) > 0:
                    ind = ind.index[-1]
                    sub_comparacao = sub_comparacao.iloc[ind + 1:, :]
                erros.append(sub_comparacao)
                if acertos <= 0:
                    self.statistics['0'].append(ID)
                else:
                    self.statistics['0<x<80'].append(ID)
        self.dfs_validados = pd.concat(self.dfs_validados)
        data_mes_vigente = (self.date_now - timedelta(10)).strftime('%Y-%m-01')
        self.dfs_validados = self.dfs_validados.query("Data >= @data_mes_vigente")
        self.dfs_validados.ID = self.dfs_validados.ID.astype(str)
        if len(erros):
            df = pd.concat(erros, ignore_index=True)
            upload_bucket(bot_name=self.bot_name).upload_parquet(df, "gabarito")
            print("created gabarito")
        print('__validate: OK')

    def create_json_daily(self):
        df_day = self.dfs_validados.copy().sort_values(by=['ID', 'Data'])
        df_day['Data'] = df_day['Data'].dt.strftime('%d-%m-%Y')
        df_day = df_day[['ID', 'Data', 'Valor']]
        df_day['ID'] = df_day['ID'].astype(str)
        df_day['Base'] = "0"
        df_day["Observacao"] = "Data Science"
        self.lista_requisicao_daily = []

        for ID in df_day.ID.unique():
            params = {'Internal': 'true', 'DadosDiarios': 'true'}
            rjson = dict()
            rjson["PercentualOutlier"] = self.PercentualOutlier
            rjson["DadosDiarios"] = df_day.query("ID == @ID").to_dict(orient='records')
            rjson["DadosDiariosAux"] = [{"ID": ID, "ExcluirHistorico": self.ExcluirHistorico, 'Origem': 'Data Science'}]
            self.lista_requisicao_daily.append({'url': 'https://api.costdrivers.com/api/Indicador', 'method': 'post',
                                                'params': params, 'json': rjson, 'ID': ID})
        print('__create_jsons: OK')

    def upload(self):
        self.create_json_daily()
        self.upload_daily = ApiAsync(True, self.lista_requisicao_daily, headers=self.headers_daily).run()

        self.df_uploaded_daily = pd.DataFrame(self.upload_daily)
        self.df_uploaded_daily['bot_name'] = self.bot_name

        self.ids_to_month = self.df_uploaded_daily.query("Success == True").ID.unique()
        print('__upload: OK')
        # self.upload_month()

    def upload_month(self):
        self.get_plataform_data()
        ID_date = self.dfs_validados.query("ID in @self.ids_to_month").groupby(["ID"], as_index=False).min()
        ID_date.Data = ID_date.Data.dt.strftime("%Y-%m-01")
        dfs = list()
        self.df_costdrivers.ID = self.df_costdrivers.ID.astype(str)
        self.df_costdrivers.Data = pd.to_datetime(self.df_costdrivers.Data, format="%d-%m-%Y")
        for _, row in ID_date.iterrows():
            sub = self.df_costdrivers.query("ID == @row['ID'] and Data >= @row['Data']").copy()
            sub.Data = sub.Data.dt.strftime('%m-%Y')
            sub = sub.groupby(["ID", "Data"], as_index=False).mean()
            dfs.append(sub)

        df = pd.concat(dfs, ignore_index=True)
        df.Data = pd.to_datetime(df.Data, format='%m-%Y')
        df.sort_values(['ID', 'Data'], ascending=[True, True], inplace=True)
        uploaded = UploadData(df=df, bot_name=self.bot_name, token_costdriver=get_token_costdrivers(), out=4)
        uploaded.upload_data()
        self.df_i, name = upload_bucket(bot_name=self.bot_name + "_mensal").upload_parquet(uploaded.df_uploaded,
                                                                                           "upload")
        print('DailyProcess.upload_month: OK!')


@for_all_methods(LogErrorDecorator)
class monthly_process_wrapper:
    def __init__(self) -> None:
        self.bot_name = self.__class__.__name__
        self.errors = {}
        self.proceed = True
        self.token_costdriver = ''
        self.init_timestamp = datetime.now()
        self.df_faltantes = pd.DataFrame()
        self.url_coleta = ''
        self.url_tratamento = ''
        self.url_validacao = ''
        self.url_upload = ''
        self.url_comparacao = ''
        self.last_step = ''
        self.validated_clean = pd.DataFrame()
        self.IDs_novos = ''
        self.statistics = None
        self.IDs_platform_date = 0
        self.url_gabarito = ''
        self.url_date_error = ''
        self.n_zero_or_enegative = 0
        self.n_null = 0
        self.n_not_number = 0
        self.n_date_gap = 0
        self.n_future = 0
        self.memory_code = ''
        self.regenerated = False
        self.code_regen_status = ""
        self.path_dir = os.path.dirname(os.path.abspath(__file__))
        self.filename = self.get_filename()
        self.schedule_interval = self.get_scheduling()
        self.bot_instance = findFailure(bot_name=self.bot_name, path_dir=self.path_dir)  # has S3 reqs
        self.cutting = 2
        self.semestral = False
        self.n_months = 6
        self.tolerancia = 0.01  # porcentagem de tolerancia entre valor coletado e valor da plataforma

    def get_filename(self):
        """Get the filename for the bot instance based on the current working directory."""
        self.path_dir = get_airflow_include_path()

        storage = AzureBlobStorage()
        data = storage.download_azure(f"documentation/{self.bot_name}.xlsx")
        
        return data

    def get_scheduling(self) -> str:

        # bucket = bucket_manager()
        # file = bucket.get_bucket_file('s3://costdrivers-mwaa/dags/scheduling.yaml')  # has S3 reqs
        file = 'scheduling.yaml'
        file = os.path.join(self.path_dir, file)
        with open(file, 'rb') as f:
            timming = yaml.load(f, Loader=yaml.FullLoader)

        schedule_interval = timming.get(self.bot_name)
        if schedule_interval is None:
            schedule_interval = "@daily"
        return schedule_interval

    def ambientacao(self, *metodos):
        self.last_step = 'ambientacao'
        for metodo in metodos:
            self.last_step = f'{metodo}'
            print('self.last_step: ', self.last_step)
            getattr(self, metodo)()
        if type(self.df_excel).__name__ == 'dict':
            df_excel = pd.DataFrame([{'ID': val, 'Nome': key} for key, val in self.df_excel.items()])
        elif type(self.df_excel).__name__ == 'DataFrame':
            df_excel = self.df_excel
        else:
            print("tipo nao esperado!!!\n", type(self.df_excel).__name__)
        self.bot_instance.ids_fonte(df_ids=df_excel)
        self.ids_fonte = self.bot_instance.parquet
        print("self.ids_fonte: ", self.ids_fonte)
        self.ids = df_excel
        # 
        init_s3 = upload_bucket(bot_name=self.bot_name)
        parquet_buffer = BytesIO()
        df_excel['bot_name'] = self.bot_name
        df_excel[['ID', 'bot_name']].to_parquet(parquet_buffer, engine='pyarrow', compression='gzip')
        name = 'excel_ids/' + self.bot_name + '.parquet'
        init_s3.AzureBlob.upload_azure(name=name, data=df_excel[['ID', 'bot_name']], extension='.parquet')
        # init_s3.s3.Object(init_s3.my_bucket, name).put(Body=parquet_buffer.getvalue())
        print("saved to s3: ", name)

    def historic_validation(self, s3, nome):
        self.last_step = 'historic_validation'
        print("nome: ", nome)
        my_bucket = "mwaa-collected-data"
        files = {}
        for file in s3.Bucket(my_bucket).objects.all():
            if (nome in file.key) and (self.bot_name in file.key):
                files[int(file.key.split("_")[0].replace(':', ''))] = file.key
                print("achado file.key: ", file.key)
        if files != {}:
            max_date_file = files[max(files.keys())]
            print("max_date_file: ", max_date_file)
            s3_response_object = s3.Object(bucket_name=my_bucket, key=max_date_file)
            object_content = s3_response_object.get()
            object_content = object_content['Body'].read()
            df = pd.read_parquet(BytesIO(object_content))
        else:
            print("nao temos o arquivo ainda!")
            df = pd.DataFrame()
        return df

    def coleta(self, *metodos):
        self.last_step = 'coleta'
        for metodo in metodos:
            self.last_step = f'{metodo}'
            getattr(self, metodo)()
        if type(self.df_colected).__name__ == 'list':
            df_colected = pd.concat(self.df_colected)
        elif type(self.df_colected).__name__ == 'DataFrame':
            df_colected = self.df_colected
        else:
            print("tipo nao esperado!!!\n", type(self.df_colected).__name__)

        init_s3 = upload_bucket(bot_name=self.bot_name)
        df_i, self.url_coleta = init_s3.upload_parquet(df_colected, "coletado")
        if len(self.errors) > 0:
            self.bot_instance.fonte_modificada(error_list=self.errors)

        if df_colected.empty:
            raise Exception('Leitura finalizada com dataframe vazio!')

        self.df_colected = df_colected

    def results_validation(self):
        df = self.df_results.copy()
        df.sort_values(by=['ID', 'Data'], inplace=True)
        results_validation_logging = {
            'zero_or_enegative': list(),
            'null': list(),
            'not_number': list(),
            'date_gap': list(),
            'future': list(),
            'possible_problem': dict(),
        }
        # has negative or zero values
        df_neg = df.query("Valor <= 0")
        if not df_neg.empty:
            ids_neg = list(df_neg.ID.unique())
            results_validation_logging['zero_or_enegative'] = ids_neg
            self.n_zero_or_enegative = len(ids_neg)
        # has null values
        df_null = df.query("Valor.isnull()")
        if not df_null.empty:
            ids_null = list(df_null.ID.unique())
            results_validation_logging['null'] = ids_null
            self.n_null = len(ids_null)
        # has not_number values
        df['values_norm'] = pd.to_numeric(df['Valor'], errors='coerce', downcast='integer')
        df_null = df.query("values_norm.isnull()")
        if not df_null.empty:
            ids_null = list(df_null.ID.unique())
            results_validation_logging['not_number'] = ids_null
            self.n_not_number = len(ids_null)
        # date gap
        df['date_gap'] = df['Data'] - df['Data'].shift(1)
        df_date_gap = df.query("date_gap > '31 days'")
        if not df_date_gap.empty:
            ids_date_gap = list(df_date_gap.ID.unique())
            results_validation_logging['date_gap'] = ids_date_gap
            self.n_date_gap = len(ids_date_gap)
        # future date
        now = datetime.now()
        df_future = df.query("Data > @now")
        if not df_future.empty:
            ids_future = list(df_future.ID.unique())
            results_validation_logging['future'] = ids_future
            self.n_future = len(ids_future)
        # futuro preenchido com zeros
        if results_validation_logging['future'] and (
                results_validation_logging['future'] == results_validation_logging['zero_or_enegative']):
            results_validation_logging['possible_problem']['futuro_preenchido_com_zeros'] = True
        # display results
        print(" -------------------- results_validation -------------------- ")
        for key, value in results_validation_logging.items():
            if value:
                print(f'{key}: {value}')
        # display problems
        for problem, value in results_validation_logging['possible_problem'].items():
            print(f'{problem}: {value}')
        print(" -------------------- results_validation -------------------- ")

    def tratamento(self, *metodos):
        self.last_step = 'tratamento'
        if self.proceed:
            for metodo in metodos:
                self.last_step = f'{metodo}'
                getattr(self, metodo)()
            self.df_results.dropna(inplace=True)
            print(" -------------------- conferir os data types -------------------- ")
            print(self.df_results.head())
            self.df_results.info()
            print(" -------------------- conferido os data types -------------------- ")
            # Converter "Data" para datetime (%Y-%m-%d), forçando erros para NaT
            self.df_results["Data"] = pd.to_datetime(self.df_results["Data"], errors="coerce", format="%d-%m-%Y")
            if self.df_results['Data'].isnull().any():
                raise ValueError("self.df_results.Data have some invalid date format. Please asset to %d-%m-%Y date format.")
            # Garantir que "Valor" seja float, forçando erros para NaN
            self.df_results["Valor"] = pd.to_numeric(self.df_results["Valor"], errors="coerce")
            if self.df_results['Valor'].isnull().any():
                raise ValueError("self.df_results.Valor have some invalid format. Please check the input values.")
            try:
                self.df_results['Data'] = self.df_results['Data'].dt.strftime('01-%m-%Y')
                self.df_results['Data'] = pd.to_datetime(self.df_results['Data'], format='%d-%m-%Y')
            except:
                raise ValueError("self.df_results.Data is not a datetime object")
            self.df_results.query("Data >= '2001-01-01'", inplace=True)
            # remove future data
            today = datetime.now().strftime('%Y-%m-%d')
            self.df_results.query(f"Data < '{today}'", inplace=True)
            self.df_results = self.df_results.groupby(["ID", "Data"], as_index=False).mean()
            # printar maior data por ID
            print(' -------------------- Conferir a maior data por ID -------------------- ')
            print(self.df_results.groupby("ID")["Data"].max().sort_values(ascending=False).head(10))
            print(' -------------------- Conferido a maior data por ID -------------------- ')
            self.df_faltantes = self.bot_instance.falha_merge(df=self.df_results)
            # self.bot_instance.fonte_atualizada(df=self.df_results)
            self.df_i, self.url_tratamento = upload_bucket(bot_name=self.bot_name).upload_parquet(self.df_results,
                                                                                                  "tratado")
        else:
            print("process bypassed by self.proceed")
        self.qtd_ids_pre_validacao = self.df_results.ID.nunique()
        self.results_validation()

    def validacao(self):
        cutting = self.cutting
        semestral = self.semestral
        n_months = self.n_months
        tolerancia = self.tolerancia
        self.last_step = 'validacao'
        # adicionado para quando tem ID novo
        self.bot_instance.df_parquet = self.bot_instance.df_parquet.rename(columns={"id": "ID"})
        ids = self.bot_instance.df_parquet.ID.astype(int).unique()
        self.df_results.ID = self.df_results.ID.astype(int)
        df_novos = self.df_results.query("ID not in @ids")
        self.df_results = self.df_results.query("ID in @ids")
        if self.proceed and (not self.df_results.empty):
            val = BotValidation(df=self.df_results, bot_name=self.bot_name, semestral=semestral, df_excel=self.df_excel,
                                n_months=n_months, tolerancia=tolerancia)
            self.validated = val.compare_dataframes(cutting=cutting)
            self.IDs_platform_date = val.IDs_platform_date
            self.url_gabarito = val.url_gabarito
            self.url_date_error = val.url_date_error
            self.token_costdriver = val.token_costdriver
            self.statistics = val.statistics
            erros = val.erros
            self.data_min = val.data_min
            ids_erro = list(erros.keys())
            self.validated_clean = self.validated.query("ID not in @ids_erro")
            self.url_comparacao = val.url_comparacao
        else:
            self.token_costdriver = get_token_costdrivers()
            self.validated_clean = self.validated = pd.DataFrame(columns=['ID', 'Data', 'Valor'])
            print("process bypassed by self.proceed")

        def resample(sub_df):
            sub_df = sub_df.drop_duplicates(subset=['Data'])
            sub_df.index = sub_df.Data
            sub_df = sub_df.resample('M').ffill()
            sub_df['Data'] = sub_df.index
            sub_df = sub_df.reset_index(drop=True)
            return sub_df

        # adicionado para quando tem ID novo
        if not df_novos.empty:
            print("Temos IDs novos!")
            dfs = list()
            df_novos = df_novos.query("Valor > 0")
            df_novos.sort_values(by=['ID', 'Data'], inplace=True)
            self.IDs_novos = df_novos.ID.unique()
            print("ID list; ", self.IDs_novos)

            print("starting resample")
            for ref in self.IDs_novos:
                sub_df = df_novos.query("ID == @ref")
                sub_df = resample(sub_df)
                dfs.append(sub_df)

            print("resample finish")
            df_novos = pd.concat(dfs)
            df_novos['Data'] = df_novos['Data'].dt.strftime('01-%m-%Y')
            df_novos['ID'] = df_novos['ID'].astype(int)
            self.validated_clean = self.validated = pd.concat([self.validated_clean, df_novos], ignore_index=True)

        self.validated['Data'] = pd.to_datetime(self.validated['Data'], format="%d-%m-%Y")
        self.validated_clean['Data'] = pd.to_datetime(self.validated_clean['Data'], format="%d-%m-%Y")
        if not self.validated.empty:
            self.df_i, self.url_validacao = upload_bucket(bot_name=self.bot_name).upload_parquet(self.validated,
                                                                                                 "validado",
                                                                                                 df_t=self.df_i)

    def upload(self, out=4):
        try:
            exit_changelog = code_regen.save_latest_changelog(bot_name=self.bot_name)
            if exit_changelog is not None:
                if exit_changelog != "":
                    self.code_regen_status = exit_changelog
                    self.regenerated = True
                    print('[CODE-REGEN] Successfully regenerated code.')
        except:
            print('[CODE-REGEN] Missing code_regen.py at same path. Passing.')
        self.last_step = 'upload'
        if self.proceed and (not self.validated_clean.empty):
            self.uploaded = UploadData(df=self.validated_clean, bot_name=self.bot_name,
                                       token_costdriver=self.token_costdriver, out=out)
            self.uploaded.upload_data()
            self.df_i, self.url_upload = upload_bucket(bot_name=self.bot_name).upload_parquet(self.uploaded.df_uploaded,
                                                                                              "upload", df_t=self.df_i)
            self.missing_ids_validation()
            self.df_uploaded = self.uploaded.df_uploaded.copy()
        else:
            self.df_uploaded = pd.DataFrame(columns=['ID', 'success'])
            print("process bypassed by self.proceed or self.validated_clean.empty")
        self.logging()

    def missing_ids_validation(self):
        self.last_step = 'missing_ids_validation'
        try:
            if (not self.df_faltantes.empty) and (
                    sum(self.df_colected.columns == ['Data', 'Referencia', 'Valor']) == 3):
                ids = self.df_faltantes['ID'].unique()
                validation = matrix_validation(lista_ids=ids)
                df = self.df_colected.copy()
                validation.possibilities_matrix(df=df)
                parametros = validation.compare()
                print('parametros:\n', parametros)
                df_i, name = upload_bucket(bot_name=self.bot_name).upload_parquet(parametros, "matrix_validation")
        except:
            pass

    def make_atualization_series(self):
        self.last_step = 'make_atualization_series'
        df = self.validated.copy()
        df_gb = df.groupby(['ID'], as_index=False).max().groupby(['Data'], as_index=False).count()
        df_gb = df_gb.rename(columns={"ID": "count"})
        df_gb.Data = df_gb.Data.dt.strftime("%Y-%m-%d")
        self.atualization_serie = [(x, y) for x, y in zip(df_gb['Data'], df_gb['count'])]
        self.df_gb = df_gb.copy()

    def make_atualization_statistics(self):
        self.last_step = 'make_atualization_statistics'
        df_gb = self.df_gb.copy()
        now = datetime.now()
        n30_days = (now - timedelta(30)).strftime("%Y-%m-01")
        n60_days = (now - timedelta(60)).strftime("%Y-%m-01")
        n90_days = (now - timedelta(90)).strftime("%Y-%m-01")
        n180_days = (now - timedelta(180)).strftime("%Y-%m-01")
        n365_days = (now - timedelta(365)).strftime("%Y-%m-01")

        self.df_30_days = df_gb.query("Data >= @n30_days")['count'].nunique()
        self.df_60_days = df_gb.query("Data >= @n60_days and Data < @n30_days")['count'].nunique()
        self.df_90_days = df_gb.query("Data >= @n90_days and Data < @n60_days")['count'].nunique()
        self.df_180_days = df_gb.query("Data >= @n180_days and Data < @n90_days")['count'].nunique()
        self.df_365_days = df_gb.query("Data >= @n365_days and Data < @n180_days")['count'].nunique()
        self.df_365_days_before = df_gb.query("Data < @n365_days")['count'].nunique()

    def get_execution_local(self):
        self.last_step = 'get_execution_local'
        if "airflow" in os.getcwd():
            local = "airflow"
        else:
            local = os.getcwd().split("\\")[2]
        return local

    def logging(self):
        # 
        def upload_error_types(row: pd.Series):
            upload_error_types_dict = {
                "ID": row['ID'],
                "msg": row['msg_uploaded_false'],
                "error": row['Error']
            }
            return str(upload_error_types_dict)

        #
        self.last_step = 'logging'
        data = self.init_timestamp.strftime("%d-%m-%YT%H:%M:%S")
        tempo_execucao = datetime.now().timestamp() - self.init_timestamp.timestamp()
        self.make_atualization_series()
        self.make_atualization_statistics()
        local = self.get_execution_local()
        df_uploaded_true = self.df_uploaded.query("success == True").copy()
        df_uploaded_false = self.df_uploaded.query("success != True").copy()
        if not df_uploaded_false.empty:
            df_uploaded_false['msg_uploaded_false'] = [
                set([sub.replace("\\", "") for sub in str(res).split('"error":')[-1].split("}")[0][2:-2].split(r"\n")])
                for res in df_uploaded_false.response]
            msgs_false = df_uploaded_false.apply(upload_error_types, axis=1).to_list()
        else:
            msgs_false = ''
        loging = {
            "bot_name": self.bot_name,
            "data": data,
            "tempo_execucao": tempo_execucao,
            "qtd_unique_referencias": self.df_colected.Referencia.nunique() if "Referencia" in self.df_colected.columns else np.nan,
            "qtd_ids_plataforma": self.ids_fonte.ID.nunique(),
            "qtd_ids_excel": self.ids.ID.nunique(),
            "qtd_ids_pre_validacao": self.qtd_ids_pre_validacao,
            "qtd_ids_reprovados": len(self.statistics['0<x<80']) if self.statistics != None else 0,
            "qtd_ids_zero": len(self.statistics['0']) if self.statistics != None else 0,
            "qtd_ids_pos_validacao": self.validated.ID.nunique(),
            "qtd_ids_upload": df_uploaded_true.ID.nunique(),
            "start_date": self.validated_clean.Data.min().strftime("%d-%m-%Y") if not pd.isna(
                self.validated_clean.Data.min()) else 0,
            "atualization_serie": self.atualization_serie,
            "30_days": self.df_30_days,
            "60_days": self.df_60_days,
            "90_days": self.df_90_days,
            "180_days": self.df_180_days,
            "365_days": self.df_365_days,
            "365_days_before": self.df_365_days_before,
            "fora_plataforma": len(self.statistics['fora_plataforma']) if self.statistics != None else 0,
            "data_divergente": len(self.statistics['data_divergente']) if self.statistics != None else 0,
            "url_coleta": self.url_coleta,
            "url_tratamento": self.url_tratamento,
            "url_comparacao": self.url_comparacao,
            "url_validacao": self.url_validacao,
            "url_upload": self.url_upload,
            "execution_local": local,
            "uploaded_false": msgs_false,
            "IDs_novos": str(list(self.IDs_novos)),
            "IDs_platform_date": self.IDs_platform_date,
            "url_gabarito": self.url_gabarito,
            "url_date_error": self.url_date_error,
            "n_zero_or_enegative": self.n_zero_or_enegative,
            "n_null": self.n_null,
            "n_not_number": self.n_not_number,
            "n_date_gap": self.n_date_gap,
            "n_future": self.n_future,
            "code_regen_status": self.code_regen_status
        }
        print('statisticas:\n', loging)
        upload_bucket(bot_name=self.bot_name).upload_json(name=f"logs/{data}_{self.bot_name}.json", dicio=loging)
        if self.statistics != None:
            print("self.statistics['fora_plataforma']\n", self.statistics['fora_plataforma'])
            print("self.statistics['data_divergente']\n", self.statistics['data_divergente'])


@for_all_methods(LogErrorDecorator)
class daily_process_wrapper:
    def __init__(self) -> None:
        self.bot_name = self.__class__.__name__
        self.bot_instance = findFailure(bot_name=self.bot_name)
        self.errors = {}
        self.proceed = True
        self.init_timestamp = datetime.now()
        self.df_faltantes = pd.DataFrame()
        self.url_coleta = ''
        self.url_tratamento = ''
        self.url_validacao = ''
        self.url_upload = ''
        self.url_comparacao = ''
        self.last_step = ''
        self.statistics = None
        self.code_regen_status = ""
        self.path_dir = os.path.dirname(os.path.abspath(__file__))
        self.filename = self.get_filename()
        self.schedule_interval = self.get_scheduling()

    def get_filename(self):
        """Get the filename for the bot instance based on the current working directory"""
        if "airflow" in os.getcwd():
            self.path_dir = f"/opt/airflow/include/"

        storage = AzureBlobStorage()
        data = storage.download_azure(f"documentation/{self.bot_name}.xlsx")
        
        return data

    def get_scheduling(self) -> str:
        # bucket = bucket_manager()
        # file = bucket.get_bucket_file('s3://costdrivers-mwaa/dags/scheduling.yaml')
        file = 'scheduling.yaml'
        file = os.path.join(self.path_dir, file)
        with open(file, 'rb') as f:
            timming = yaml.load(f, Loader=yaml.FullLoader)

        schedule_interval = timming.get(self.bot_name)
        if schedule_interval is None:
            schedule_interval = "@daily"
        return schedule_interval

    def ambientacao(self, *metodos):
        self.last_step = 'ambientacao'
        for metodo in metodos:
            getattr(self, metodo)()
        if type(self.df_excel).__name__ == 'dict':
            df_excel = pd.DataFrame([{'ID': val, 'Nome': key} for key, val in self.df_excel.items()])
        elif type(self.df_excel).__name__ == 'DataFrame':
            df_excel = self.df_excel
        else:
            print("tipo nao esperado!!!\n", type(self.df_excel).__name__)
        self.bot_instance.ids_fonte(df_ids=df_excel)
        self.ids_fonte = self.bot_instance.parquet
        self.ids = df_excel
        print("self.ids_fonte: ", self.ids_fonte)
        init_s3 = upload_bucket(bot_name=self.bot_name)
        parquet_buffer = BytesIO()
        df_excel['bot_name'] = self.bot_name
        df_excel[['ID', 'bot_name']].to_parquet(parquet_buffer, engine='pyarrow', compression='gzip')
        name = 'excel_ids/' + self.bot_name + '.parquet'
        init_s3.AzureBlob.upload_azure(name=name, data=df_excel[['ID', 'bot_name']], extension='.parquet')
        # init_s3.s3.Object(init_s3.my_bucket, name).put(Body=parquet_buffer.getvalue())
        print("saved to s3: ", name)

    def historic_validation(self, s3, nome):
        self.last_step = 'historic_validation'
        try:
            s3_response_object = s3.Object(bucket_name="mwaa-collected-data", key=nome)
            object_content = s3_response_object.get()
            object_content = object_content['Body'].read()
            df = pd.read_parquet(BytesIO(object_content))
        except:
            print("nao temos o arquivo ainda!")
            df = pd.DataFrame()
        return df

    def coleta(self, *metodos):
        self.last_step = 'coleta'
        for metodo in metodos:
            getattr(self, metodo)()
        if type(self.df_colected).__name__ == 'list':
            df_colected = pd.concat(self.df_colected)
        elif type(self.df_colected).__name__ == 'DataFrame':
            df_colected = self.df_colected
        else:
            print("tipo nao esperado!!!\n", type(self.df_colected).__name__)

        init_s3 = upload_bucket(bot_name=self.bot_name)
        # df_hist = self.historic_validation(init_s3.s3, f"coletado/{self.bot_name}.parquet")
        # df_hist = df_hist.reset_index(drop=True)
        self.df_colected = df_colected.reset_index(drop=True)
        df_i, self.url_coleta = init_s3.upload_parquet(df_colected, "coletado")
        if len(self.errors) > 0:
            self.bot_instance.fonte_modificada(error_list=self.errors)
        self.df_colected = df_colected

    def tratamento(self, *metodos):
        self.last_step = 'tratamento'
        if self.proceed:
            for metodo in metodos:
                getattr(self, metodo)()
            self.df_faltantes = self.bot_instance.falha_merge(df=self.df_results)
            
            self.df_results.dropna(inplace=True)
            print(" -------------------- conferir os data types -------------------- ")
            print(self.df_results.tail())
            self.df_results.info()
            print(" -------------------- conferido os data types -------------------- ")
            
            # Converter "Data" para datetime (%Y-%m-%d), forçando erros para NaT
            self.df_results["Data"] = pd.to_datetime(self.df_results["Data"], errors="coerce", format="%d-%m-%Y")
            if self.df_results['Data'].isnull().any():
                raise ValueError("self.df_results.Data have some invalid date format. Please asset to %d-%m-%Y date format.")
            
            # Garantir que "Valor" seja float, forçando erros para NaN
            self.df_results["Valor"] = pd.to_numeric(self.df_results["Valor"], errors="coerce")
            if self.df_results['Valor'].isnull().any():
                raise ValueError("self.df_results.Valor have some invalid format. Please check the input values.")
            
            self.df_results.query("Data >= '2001-01-01'", inplace=True)
            
            # remove future data
            # printar maior data por ID
            print(' -------------------- Conferir a maior data por ID -------------------- ')
            print(self.df_results.groupby("ID")["Data"].max().sort_values(ascending=False).head(10))
            print(' -------------------- Conferido a maior data por ID -------------------- ')
            
            self.df_i, self.url_tratamento = upload_bucket(bot_name=self.bot_name).upload_parquet(self.df_results, "tratado")
            
        else:
            print("process bypassed by self.proceed")

    def validacao(self, out=4.44, tolerancia=0.01):
        self.last_step = 'validacao'
        if self.proceed:
            self.dp = DailyProcess(self.df_results, self.__class__.__name__, out=out, df_excel=self.df_excel,
                                   tolerancia=tolerancia)
            self.dp.validate()
            self.df_i, self.url_validacao = upload_bucket(bot_name=self.bot_name).upload_parquet(self.dp.dfs_validados,
                                                                                                 "validado",
                                                                                                 df_t=self.df_i)
            self.url_comparacao = self.dp.url_comparacao
        else:
            print("process bypassed by self.proceed")

    def upload(self):
        try:
            current_bot_name = self.bot_name
            exit_changelog = code_regen.save_latest_changelog(bot_name=current_bot_name)
            if exit_changelog is not None:
                if exit_changelog != "":
                    self.code_regen_status = exit_changelog
                    print('[CODE-REGEN] Successfully regenerated code.')
        except:
            print('[CODE-REGEN] Missing code_regen.py at same path. Passing.')
        self.last_step = 'upload'
        if self.proceed:
            self.dp.upload()
            self.dp.df_uploaded_daily = self.dp.df_uploaded_daily[['ID', 'Msg']]
            self.dp.df_uploaded_daily.ID = self.dp.df_uploaded_daily.ID.astype(int)
            self.dp.df_uploaded_daily.Msg = self.dp.df_uploaded_daily.Msg.astype(str)
            self.df_uploaded_error = self.dp.df_uploaded_daily.query("Msg != 'Dados diários atualizados com sucesso!'")
            self.df_uploaded_daily = self.dp.df_uploaded_daily.query("Msg == 'Dados diários atualizados com sucesso!'")
            self.df_i, self.url_upload = upload_bucket(bot_name=self.bot_name).upload_parquet(
                self.dp.df_uploaded_daily[['ID', 'Msg']], "upload", df_t=self.df_i)
            self.logging()
            if not self.df_uploaded_error.empty:
                upload_bucket(bot_name=self.bot_name).upload_parquet(self.df_uploaded_error[['ID', 'Msg']],
                                                                     "error_upload", df_t=self.df_i)
                pass
        else:
            print("process bypassed by self.proceed or self.validated_clean.empty")

    def make_atualization_series(self):
        self.last_step = 'make_atualization_series'
        if (self.dp != None):
            df = self.dp.dfs_validados.copy()
        else:
            df = self.validated_clean
        df_gb = df.groupby(['ID'], as_index=False).max().groupby(['Data'], as_index=False).count()
        df_gb = df_gb.rename(columns={"ID": "count"})
        df_gb.Data = df_gb.Data.dt.strftime("%Y-%m-%d")
        atualization_serie = [(x, y) for x, y in zip(df_gb['Data'], df_gb['count'])]
        return atualization_serie

    def get_execution_local(self):
        self.last_step = 'get_execution_local'
        if "airflow" in os.getcwd():
            local = "airflow"
        else:
            local = os.getcwd().split("\\")[2]
        return local

    def logging(self):
        if (self.dp != None):
            self.df_uploaded = self.dp.df_uploaded_daily

        #
        def upload_error_types(row: pd.Series):
            upload_error_types_dict = {
                "ID": row['ID'],
                "msg": row['msg_uploaded_false'],
            }
            return str(upload_error_types_dict)

        #
        if 'Msg' not in self.df_uploaded.columns:
            self.df_uploaded['Msg'] = ''
        df_uploaded_false = self.df_uploaded.query("Msg != 'Dados diários atualizados com sucesso!'").copy()
        if not df_uploaded_false.empty:
            df_uploaded_false['msg_uploaded_false'] = [res.replace("\\", "") for res in df_uploaded_false.Msg]
            msgs_false = df_uploaded_false.apply(upload_error_types, axis=1).to_list()
        else:
            msgs_false = ''
        # 
        self.last_step = 'logging'
        data = self.init_timestamp.strftime("%d-%m-%YT%H:%M:%S")
        tempo_execucao = datetime.now().timestamp() - self.init_timestamp.timestamp()
        local = self.get_execution_local()
        loging = {
            "bot_name": self.bot_name,
            "data": data,
            "tempo_execucao": tempo_execucao,
            "qtd_unique_referencias": self.df_colected.Referencia.nunique() if "Referencia" in self.df_colected.columns else np.nan,
            "qtd_ids_plataforma": self.ids_fonte.ID.nunique(),
            "qtd_ids_excel": self.ids.ID.nunique(),
            "qtd_ids_pre_validacao": self.df_results.ID.nunique(),
            "qtd_ids_reprovados": len(self.dp.statistics['0<x<80']) if (self.dp != None) else 0,
            "qtd_ids_zero": len(self.dp.statistics['0']) if (self.dp != None) else 0,
            "qtd_ids_pos_validacao": self.dp.dfs_validados.ID.nunique() if (
                        self.dp != None) else self.validated_clean.ID.nunique(),
            "qtd_ids_upload": self.dp.df_uploaded_daily.ID.nunique() if (
                        self.dp != None) else self.df_uploaded.ID.nunique(),
            "start_date": self.dp.dfs_validados.Data.min().strftime("%d-%m-%Y") if (
                        self.dp != None) else self.validated_clean.Data.min().strftime("%d-%m-%Y"),
            "atualization_serie": self.make_atualization_series(),
            "fora_plataforma": len(self.dp.statistics['fora_plataforma']) if (self.dp != None) else 0,
            "data_divergente": len(self.dp.statistics['data_divergente']) if (self.dp != None) else 0,
            "url_coleta": self.url_coleta,
            "url_tratamento": self.url_tratamento,
            "url_comparacao": self.url_comparacao,
            "url_validacao": self.url_validacao,
            "url_upload": self.url_upload,
            "execution_local": local,
            "uploaded_false": msgs_false,
            "code_regen_status": self.code_regen_status,
            "bypass_validation": self.dp.bypass_validation if self.dp != None else None
        }
        print('statisticas:\n', loging)
        upload_bucket(bot_name=self.bot_name).upload_json(name=f"logs/{data}_{self.bot_name}.json", dicio=loging)
        if (self.dp != None):
            print("self.statistics['fora_plataforma']\n", self.dp.statistics['fora_plataforma'])
            print("self.statistics['data_divergente']\n", self.dp.statistics['data_divergente'])


class COMEX_wrapper:
    def __init__(self) -> None:
        pass

    def coleta(self, *metodos):
        for metodo in metodos:
            getattr(self, metodo)()
        self.df_impo_colect
        self.df_expo_colect

    def tratamento(self, *metodos):
        for metodo in metodos:
            getattr(self, metodo)()
        self.df_impo
        self.df_expo

    def validacao(self):
        print("process_validacao(self.df_impo)")
        print("process_validacao(self.df_expo)")
        self.df_expo_val
        self.df_impo_val

    def upload(self):
        print("process_upload(self.df_expo_val)")
        self.df_impo_uploaded
        self.df_expo_uploaded


'''


ajust do logging:

1. Qual era o problema identificado?
2. Houve algum erro na documentação?
3. Evidências:

requisição "manual"

dados = lista_requisicao[0]
response = requests.put(dados['url'], json=dados['data'], headers=headers)

'''


class pseudo_validation:
    def __init__(self, name):
        self.dfs = list()
        self.descs = list()
        self.name = name
        self.bucket_root = "mwaa-collected-data"
        self.aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
        self.aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
        self.region = "eu-central-1"

    #
    def add_df(self, df):
        self.dfs.append(df)

    #
    def add_df_desc(self, df, desc):
        self.dfs.append(df)
        self.descs.append(desc)

    #
    def create_mail(self, type='old'):
        if type == 'old':
            body = MIMEText(
                f'Bom dia, Marcio!\nSegue em anexo os dados brutos do robo {self.name}.\n\nAtt,\nBOT {self.name}.DG_validation')
        else:
            boy_text = ' '.join(self.names)
            text = f'Bom dia, Marcio!\n\nSegue em anexo os dados brutos do robo {self.name}.\n\nAtt,\nBOT {self.name}.DG_validation\n file names in S3: {boy_text}'
            body = MIMEText(text)
        # 
        msg = MIMEMultipart()
        msg.attach(body)
        # 
        if type == 'old':
            for i, tp in enumerate(zip(self.dfs, self.descs)):
                df, desc = tp
                if df is not None:
                    attachment = MIMEApplication(df.to_csv(index=False).encode(), _subtype='csv')
                    attachment.add_header('content-disposition',
                                          'attachment', filename=f'{desc}_{i}.csv')
                    msg.attach(attachment)
            msg.attach(attachment)
            # 
        msg['Subject'] = f'{self.name} - {datetime.now().strftime("%Y-%m-%d")}'
        return msg

    #
    def save_s3(self, out_type='.parquet'):
        # self.names = list()
        # s3 = boto3.resource(
        #     "s3",
        #     aws_access_key_id=self.aws_access_key_id,
        #     aws_secret_access_key=self.aws_secret_access_key,
        #     region_name=self.region
        # )
        # pre_name = f'coleta-bruta/{self.name}/'
        # for i, tp in enumerate(zip(self.dfs, self.descs)):
        #     df, desc = tp
        #     if df is not None:
        #         dfs = list()
        #         names = list()
        #         if type(df).__name__ == 'dict':
        #             for k, v in df.items():
        #                 dfs.append(v)
        #                 names.append(k)
        #         elif type(df).__name__ == 'DataFrame':
        #             dfs.append(df)
        #             names.append('-')
        #         for df, name in zip(dfs, names):
        #             for col in df.columns:
        #                 df[col] = df[col].astype(str)
        #             parquet_buffer = BytesIO()
        #             name = pre_name + desc + '_' + name + '_' + str(i) + out_type
        #             if out_type == '.parquet':
        #                 df.to_parquet(parquet_buffer, engine='pyarrow', compression='gzip', index=False)
        #                 s3.Object(self.bucket_root, name).put(Body=parquet_buffer.getvalue())
        #             elif out_type == '.csv':
        #                 df.to_csv(parquet_buffer, index=False)
        #                 s3.Object(self.bucket_root, name).put(Body=parquet_buffer.getvalue())
        #             self.names.append(name)
        # self.create_mail(type='new')
        pass

    def send_mail(self):
        msg = self.create_mail()
        # 
        smtp_server = 'smtp.gmail.com'
        smtp_port = 587
        smtp_username = 'datamark.status@gmail.com'
        smtp_password = 'alrxebtemhdcyiqd'
        smtp_from = 'datamark.status@gmail.com'
        smtp_to = [
            'gabriella.reis@gep.com',
            'marcio.junior@gep.com'
        ]
        # 
        server = smtplib.SMTP(smtp_server, smtp_port)
        server.starttls()
        server.login(smtp_username, smtp_password)
        server.sendmail(smtp_from, smtp_to, msg.as_string())
        server.quit()


class airflow_API:
    def __init__(self):
        username = 'api_user'
        password = 'api_user@2025'
        self.auth = (username, password)
        self.endpoint = 'https://airflowcostdrivers.gep.com/api/v1'
        self.headers = {
            'Content-Type': 'application/json',
            'accept': 'application/json'
        }
    #
    def get_dags(self):
        dfs = list()
        n = 0
        while True:
            params = {
                'limit': 100,
                'only_active': 'true',
                'offset': n
            }
            # 
            response = requests.get(
                f'{self.endpoint}/dags',
                params=params,
                headers=self.headers,
                auth=self.auth,
            )
            # 
            json_response = response.json()
            df = pd.DataFrame(json_response['dags'])
            dfs.append(df)
            # 
            if df.shape[0] < 100:
                break
            else:
                n += 100
        df = pd.concat(dfs, ignore_index=True)
        return df
    #
    def start_dag(self, dag_id):
        def current_date_time():
            # Obter a data e hora atuais
            current_datetime = datetime.utcnow()
            # Formatar a data e hora no formato desejado
            formatted_datetime = current_datetime.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
            return formatted_datetime
        #
        logical_date = current_date_time()
        data = {
            "conf": {},
            "dag_run_id": f"{dag_id}_{logical_date}",
            "logical_date": logical_date
        }
        response = requests.post(
            f'{self.endpoint}/dags/{dag_id}/dagRuns',
            headers=self.headers,
            json=data,
            auth=self.auth,
        )
        print(f"Status {dag_id}: ", response.status_code, ' response: ', response.text)
    # 
    def pause_all_dags(self):
        df_dags = self.get_dags()
        df_dags.query("is_paused == False", inplace=True)
        params = {
            'update_mask': 'is_paused',
            }
        json_data = {
            'is_paused': True,
            }
        for dag_id in df_dags.dag_id.unique():
            response = requests.patch(
                f'{self.endpoint}/dags/{dag_id}',
                params=params,
                headers=self.headers,
                json=json_data,
                auth=self.auth,
                )
            print(f"Status: ", response.status_code, ' response: ', response.text)
    # 
    def play_all_dags(self):
        df_dags = self.get_dags()
        df_dags.query("is_paused == False", inplace=True)
        for dag_id, _ in df_dags.groupby("dag_id"):
            self.start_dag(dag_id)