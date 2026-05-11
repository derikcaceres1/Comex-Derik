"""
Verifica se os dados do gold foram recebidos pela API costdrivers.
Compara os últimos valores do gold com o que a API retorna para os mesmos IDs.
"""
import sys
import pathlib
import pandas as pd

_comex_root = pathlib.Path(__file__).parent.parent.parent
sys.path.insert(0, str(_comex_root))

from library.costdrivers import ApiAsync, get_token_costdrivers

# --- Carrega gold local para pegar amostra de IDs ---
gold_path = _comex_root / "OM" / "dados" / "BRA" / "gold" / "gold_2026-04-14.parquet"
gold = pd.read_parquet(gold_path)

# Pega 10 IDs de amostra (5 importação com CIF, 5 exportação sem CIF)
ids_com_cif    = gold[gold['Valor_Cif'].notna()]['ID'].unique()[:5].tolist()
ids_sem_cif    = gold[gold['Valor_Cif'].isna()]['ID'].unique()[:5].tolist()
ids_amostra    = ids_com_cif + ids_sem_cif

print(f"IDs amostra (import): {ids_com_cif}")
print(f"IDs amostra (export): {ids_sem_cif}")
print()

# --- Consulta API ---
endpoint_cost = 'https://api-costdrivers.gep.com/costdrivers-api'
token = get_token_costdrivers()
headers = {'Authorization': f'Bearer {token}'}

from datetime import datetime, timedelta
data_max = datetime.now().strftime('%d-%m-%Y')
data_min = (datetime.now() - timedelta(days=365)).strftime('%d-%m-%Y')

lista_req = [{
    'url': f'{endpoint_cost}/api/v1/DataScience/option/11',
    'method': 'get',
    'params': {
        'ids': ','.join(map(str, ids_amostra)),
        'dataCalculo1': data_min,
        'dataCalculo2': data_max
    }
}]

print(f"Consultando API para {len(ids_amostra)} IDs (período {data_min} → {data_max})...")
resps = ApiAsync(True, lista_req, headers=headers).run()

if not resps or resps[0] is None:
    print("ERRO: API não retornou dados.")
    sys.exit(1)

resp = resps[0]
if 'Error' in str(resp):
    print(f"ERRO na resposta: {resp}")
    sys.exit(1)

# --- Monta DataFrame da API ---
if isinstance(resp, list):
    df_api = pd.DataFrame(resp)
elif isinstance(resp, dict) and 'result' in resp:
    df_api = pd.DataFrame(resp['result'])
else:
    print(f"Formato inesperado: {type(resp)}")
    print(resp)
    sys.exit(1)

print(f"\nAPI retornou {len(df_api)} registros para os IDs consultados.")
print(f"Colunas: {list(df_api.columns)}")
print()

if df_api.empty:
    print("AVISO: API retornou DataFrame vazio — dados podem nao ter sido persistidos.")
    sys.exit(0)

# Normaliza nomes de colunas
col_map = {}
for c in df_api.columns:
    c_lower = c.lower()
    if 'indiceprincipal' in c_lower or c_lower == 'id':
        col_map[c] = 'ID'
    elif 'dataindice' in c_lower or 'data' in c_lower:
        col_map[c] = 'Data'
    elif 'valor' in c_lower:
        col_map[c] = 'Valor'
df_api = df_api.rename(columns=col_map)

if 'Data' in df_api.columns:
    df_api['Data'] = pd.to_datetime(df_api['Data'], errors='coerce')

# --- Compara com gold local ---
gold_amostra = gold[gold['ID'].isin(ids_amostra)].copy()
gold_amostra['Data'] = pd.to_datetime(gold_amostra['Data'])

print("=== COMPARAÇÃO (valor de Março/2026 no gold vs API) ===")
print("Nota: API pode ter datas mais recentes que o gold — isso e normal.\n")

ids_vistos = set()
for id_val in ids_amostra:
    if id_val in ids_vistos:
        continue
    ids_vistos.add(id_val)

    # Pega valor de Março/2026 no gold
    gold_id = gold_amostra[
        (gold_amostra['ID'] == id_val) &
        (gold_amostra['Data'].dt.to_period('M') == '2026-03')
    ]

    # Pega valor de Março/2026 na API
    api_id = pd.DataFrame()
    if 'ID' in df_api.columns and 'Data' in df_api.columns:
        api_id = df_api[
            (df_api['ID'] == id_val) &
            (df_api['Data'].dt.to_period('M') == '2026-03')
        ]

    gold_valor = round(float(gold_id['Valor'].iloc[0]), 4) if not gold_id.empty else 'SEM DADO'
    api_valor  = round(float(api_id['Valor'].iloc[0]), 4) if not api_id.empty else 'NAO ENCONTRADO'

    if api_valor == 'NAO ENCONTRADO':
        status = "FALHOU — Março não chegou na API"
    elif abs(float(gold_valor) - float(api_valor)) < 0.01:
        status = "OK — dado correto"
    else:
        status = f"VALOR DIFERENTE (diferença: {abs(float(gold_valor)-float(api_valor)):.4f})"

    print(f"  ID {id_val:>6} | Gold Mar/2026: {gold_valor} | API Mar/2026: {api_valor} | {status}")
