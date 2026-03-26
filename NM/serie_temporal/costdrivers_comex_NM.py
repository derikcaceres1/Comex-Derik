"""
Framework COMEX com Nova Metodologia (NM).

Este módulo implementa um framework baseado na estrutura de costdrivers_comex.py,
mas utilizando a nova metodologia estatística descrita em pipeline.py.

A nova metodologia utiliza:
- STL (Seasonal-Trend decomposition using Loess) para detecção de outliers
- Top N Percent para agregação de dados por país
- Cálculo de alpha (razão CIF/FOB) para valores CIF
- Interpolação final usando IQR
- Correção de valores negativos e ajustes no último mês

Fluxo de execução:
    collect() -> update_historical() -> normalize_historical() -> calculate() -> upload()
"""

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
import gc
import ast
from statsmodels.tsa.seasonal import STL

# Adicionar comex/ e comex/library/ ao sys.path:
# - comex/         → permite "from library.costdrivers import ..."
# - comex/library/ → permite "from BlobStorage_API import ..." (import direto usado internamente)
project_root = Path(__file__).parent.parent.parent  # NM/serie_temporal/ → NM/ → comex/
library_path = project_root / "library"
for _p in [str(project_root), str(library_path)]:
    if _p not in sys.path:
        sys.path.insert(0, _p)

from library.costdrivers import ApiAsync

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# Constante para agrupamento
group_cols = ['IDIndicePrincipal', 'Data']

# ============= FUNÇÕES DA NOVA METODOLOGIA =============

def optimize_dtypes(df):
    """
    Otimiza os tipos de dados numéricos (int e float) em um DataFrame para economizar memória.

    Parâmetros:
        - df (pd.DataFrame): DataFrame a ser otimizado.

    Retorna:
        - df (pd.DataFrame): Novo DataFrame com tipos otimizados.
        - memory_reduction (float): Porcentagem de memória economizada.
    """
    start_memory = df.memory_usage(deep=True).sum() / 1024 ** 2  # Memória inicial em MB

    for col in df.select_dtypes(include=['int', 'float']).columns:
        col_min = df[col].min()
        col_max = df[col].max()

        if pd.api.types.is_integer_dtype(df[col]):
            # Encontrar o menor tipo inteiro que suporta os valores
            if col_min >= np.iinfo(np.int8).min and col_max <= np.iinfo(np.int8).max:
                df[col] = df[col].astype(np.int8)
            elif col_min >= np.iinfo(np.int16).min and col_max <= np.iinfo(np.int16).max:
                df[col] = df[col].astype(np.int16)
            elif col_min >= np.iinfo(np.int32).min and col_max <= np.iinfo(np.int32).max:
                df[col] = df[col].astype(np.int32)
            else:
                df[col] = df[col].astype(np.int64)
        elif pd.api.types.is_float_dtype(df[col]):
            # Reduzir floats para float32, se possível
            if col_min >= np.finfo(np.float32).min and col_max <= np.finfo(np.float32).max:
                df[col] = df[col].astype(np.float32)
            else:
                df[col] = df[col].astype(np.float64)

    end_memory = df.memory_usage(deep=True).sum() / 1024 ** 2  # Memória final em MB
    memory_reduction = 100 * (start_memory - end_memory) / start_memory
    print(f'Memória reduzida de {start_memory:.2f} MB para {end_memory:.2f} MB ({memory_reduction:.2f}%).')
    return df

def reindex_series(series, date_column, value_column):
    """
    Reindex the series to ensure a complete date range and handle NaN values.
    
    Parameters:
        series (pd.DataFrame): The input series with date index.
        date_column (str): The name of the date column.
        value_column (str): The name of the value column.
    
    Returns:
        pd.DataFrame: The reindexed series with filled NaN values.
    """
    IDIndicePrincipal = series['IDIndicePrincipal'].iloc[0] if 'IDIndicePrincipal' in series.columns else None
    series.set_index(date_column, inplace=True)
    
    # create a full date range from the minimum to maximum date in the series
    full_index = pd.date_range(start=series.index.min(), end=series.index.max(), freq='MS')
    
    # Reindex
    series = series[~series.index.duplicated(keep='first')]
    series = series.reindex(full_index)

    # replace 0 values with NaN
    series[value_column].replace(0, np.nan, inplace=True)
    series['IDIndicePrincipal'] = IDIndicePrincipal
    
    return series

def fill_tail_nan(series, value_column):
    """
    Fill NaN values in the series using a rolling window approach.
    
    Parameters:
        series (pd.DataFrame): The input series with date index.
        date_column (str): The name of the date column.
        value_column (str): The name of the value column.
    
    Returns:
        pd.DataFrame: The series with NaN values filled.
    """
    # Copy values to avoid modifying original series
    values = series[value_column].copy()

    # Check if the last values are NaN (tail sequence)
    nan_tail_start = None
    for i in reversed(range(len(values))):
        if pd.isna(values.iloc[i]):
            nan_tail_start = i
        else:
            break

    if nan_tail_start is not None:
        for i in range(nan_tail_start, len(values)):
            window = values.iloc[max(0, i - 3):i]
            valid = window.dropna()
            if len(valid) > 0:
                values.iloc[i] = valid.mean()
            # If no valid value, keep NaN

    # Assign filled values back to the series
    series[value_column] = values

    if not series[series['IDIndicePrincipal'].isnull()].empty:
        print("fill_tail_nan.Warning: Series contains NaN values in 'IDIndicePrincipal': ", series['IDIndicePrincipal'].unique())
    return series

def interpolate_series(series, date_column, value_column):
    """
    Interpolate missing values in the series using linear interpolation.
    
    Parameters:
        series (pd.DataFrame): The input series with date index.
        value_column (str): The name of the value column.
    
    Returns:
        pd.DataFrame: The series with NaN values interpolated.
    """
    # Interpolate missing values
    series[value_column] = series[value_column].interpolate(method='linear')
    
    series = series.reset_index().rename(columns={'index': date_column})
    
    if not series[series[value_column].isnull()].empty:
        print(f"interpolate_series.Warning: Series contains NaN values in '{value_column}': ", series[value_column].unique())
    return series

def preprocess_data(series, date_column, value_column):
    """
    Preprocess the time series data by reindexing, filling NaN values, and interpolating.
    
    Parameters:
        series (pd.DataFrame): The input series with date index.
        date_column (str): The name of the date column.
        value_column (str): The name of the value column.
        id_column (str, optional): The name of the ID column.
        Type_col (str, optional): The name of the Type column.
    
    Returns:
        pd.DataFrame: The preprocessed series.
    """
    
    series = reindex_series(series, date_column, value_column)
    series = fill_tail_nan(series, value_column)
    series = interpolate_series(series, date_column, value_column)

    return series

def top_n_percent(df: pd.DataFrame, threshold: float) -> pd.DataFrame:
    """
    Calcula estatísticas comerciais por grupo e país, retornando apenas os grupos
    que atingem um percentual mínimo de participação no peso total.
    
    A função agrupa dados por IDIndicePrincipal, CO_ANO e CO_MES, calcula totais
    por país dentro de cada grupo, ordena por peso decrescente, calcula valores
    acumulativos e identifica quais grupos atingem o threshold especificado.
    
    Args:
        df (pd.DataFrame): DataFrame contendo dados comerciais com as colunas obrigatórias:
                          - IDIndicePrincipal: Identificador principal
                          - CO_ANO: Código do ano
                          - CO_MES: Código do mês
                          - pais_id: Código do país
                          - valor: Valor FOB
                          - peso: Peso líquido em kg
        threshold (float): Valor decimal entre 0 e 1 representando o percentual mínimo
                          de participação no peso total (ex: 0.8 para 80%)
    
    Returns:
        pd.DataFrame: DataFrame filtrado contendo apenas grupos que atingem o threshold,
                     com as seguintes colunas:
                     - IDIndicePrincipal, Data: Chaves do grupo
                     - FOB_100: Preço médio FOB do grupo total (valor/peso)
                     - FOB_80: Preço médio FOB acumulativo
                     - kg_ratio: Proporção do peso acumulativo em relação ao total
                     - pais_id_CUMUL: Lista acumulativa de códigos de países
    
    Raises:
        ValueError: Se o DataFrame estiver vazio ou se colunas obrigatórias estiverem ausentes
        TypeError: Se threshold não for numérico
        ValueError: Se threshold não estiver entre 0 e 1
    
    Example:
        >>> df = pd.DataFrame({
        ...     'IDIndicePrincipal': [1, 1, 1],
        ...     'CO_ANO': [2023, 2023, 2023],
        ...     'CO_MES': [1, 1, 1],
        ...     'pais_id': ['BR', 'US', 'CN'],
        ...     'valor': [1000, 2000, 3000],
        ...     'peso': [100, 150, 200]
        ... })
        >>> result = top_n_percent(df, 0.8)
    """
    # Validações de entrada
    if not isinstance(df, pd.DataFrame):
        raise TypeError("O parâmetro 'df' deve ser um pandas DataFrame")
    
    if df.empty:
        raise ValueError("DataFrame não pode estar vazio")
    
    if not isinstance(threshold, (int, float)):
        raise TypeError("O parâmetro 'threshold' deve ser numérico")
    
    if not 0 <= threshold <= 1:
        raise ValueError("O parâmetro 'threshold' deve estar entre 0 e 1")
    
    # Verificar colunas obrigatórias
    required_columns = ['IDIndicePrincipal', 'Data', 'pais_id', 'valor', 'frete', 'seguro', 'peso']
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        raise ValueError(f"Colunas obrigatórias ausentes: {missing_columns}")
    
    # Verificar se há valores nulos nas colunas críticas
    critical_columns = ['valor', 'peso']
    if df[critical_columns].isnull().any().any():
        raise ValueError("Valores nulos encontrados nas colunas valor ou peso")
    
    df['valor_cif'] = df['valor'] + df['frete'] + df['seguro']
    
    # Definir colunas de agrupamento
    grupo_cols = ['IDIndicePrincipal', 'Data']
    pais_cols = grupo_cols + ['pais_id']
    
    # Calcular totais por grupo de forma mais eficiente
    total_por_grupo = df.groupby(grupo_cols).agg(
        total_cif=('valor_cif', 'sum'),
        total_fob=('valor', 'sum'),
        total_kg=('peso', 'sum')
    ).reset_index()
    
    # total por país cada grupo
    por_pais = df.groupby(pais_cols).agg(
        cif_by_country=('valor_cif', 'sum'),
        fob_by_country=('valor', 'sum'),
        kg_by_country=('peso', 'sum')
    ).reset_index()
    
    # Merge otimizado
    por_pais = por_pais.merge(total_por_grupo, on=grupo_cols, how='left')
    
    # Ordenar uma única vez e calcular valores acumulativos
    por_pais = por_pais.sort_values(grupo_cols + ['kg_by_country'], ascending=[True, True, False])
    
    # Calcular valores acumulativos de forma vectorizada
    por_pais['cum_cif_by_country'] = por_pais.groupby(grupo_cols)['cif_by_country'].cumsum()
    por_pais['cum_fob_by_country'] = por_pais.groupby(grupo_cols)['fob_by_country'].cumsum()
    por_pais['cum_kg_by_country'] = por_pais.groupby(grupo_cols)['kg_by_country'].cumsum()
    
    # Calcular métricas principais
    # Usar np.divide para evitar divisão por zero
    por_pais['FOB_100'] = np.divide(por_pais['total_fob'], por_pais['total_kg'], 
        out=np.zeros_like(por_pais['total_fob'], dtype=np.float32), 
        where=por_pais['total_kg']!=0)
    
    por_pais['FOB_80'] = np.divide(por_pais['cum_fob_by_country'], por_pais['cum_kg_by_country'],
        out=np.zeros_like(por_pais['cum_fob_by_country'], dtype=np.float32),
        where=por_pais['cum_kg_by_country']!=0)

    por_pais['CIF_80'] = np.divide(por_pais['cum_cif_by_country'], por_pais['cum_kg_by_country'],
        out=np.zeros_like(por_pais['cum_cif_by_country'], dtype=np.float32),
        where=por_pais['cum_kg_by_country']!=0)
    
    por_pais['kg_ratio'] = np.divide(por_pais['cum_kg_by_country'], por_pais['total_kg'],
        out=np.zeros_like(por_pais['cum_kg_by_country'], dtype=np.float32),
        where=por_pais['total_kg']!=0)
    
    # Converter pais_id para string de forma mais eficiente
    por_pais['pais_id'] = por_pais['pais_id'].astype(str)
    
    # Função otimizada para acumular códigos de país
    def acumular_codigos_pais(codigos: pd.Series) -> List[List[str]]:
        """Acumula lista de códigos de país de forma eficiente."""
        codigos_list = codigos.tolist()
        result = []
        accumulated = []
        for codigo in codigos_list:
            accumulated = accumulated + [codigo]
            result.append(accumulated.copy())
        return result
    
    # Filtrar e limpar dados finais
    resultado = por_pais[por_pais['kg_ratio'] >= threshold].copy()
    
    if resultado.empty:
        # Retornar DataFrame vazio com estrutura correta
        return pd.DataFrame(columns=grupo_cols + ['FOB_100', 'FOB_80', 'kg_ratio', 'pais_id_CUMUL'])
    
    # Ordenar e manter apenas o primeiro registro por grupo
    resultado = resultado.sort_values(grupo_cols + ['kg_ratio'], ascending=True)
    resultado = resultado.drop_duplicates(subset=grupo_cols, keep='first')
    
    # Calcular pais_id_CUMUL apenas para as linhas selecionadas (uma por grupo)
    # usando merge vetorizado para evitar apply em todo o DataFrame
    pais_threshold = resultado[grupo_cols + ['cum_kg_by_country']].rename(
        columns={'cum_kg_by_country': 'threshold_cum_kg'}
    )
    por_pais_contrib = por_pais.merge(pais_threshold, on=grupo_cols, how='inner')
    por_pais_contrib = por_pais_contrib[
        por_pais_contrib['cum_kg_by_country'] <= por_pais_contrib['threshold_cum_kg'] + 1e-6
    ]
    pais_cumul = (
        por_pais_contrib
        .groupby(grupo_cols)['pais_id']
        .apply(lambda x: str(x.tolist()))
        .reset_index()
        .rename(columns={'pais_id': 'pais_id_CUMUL'})
    )
    resultado = resultado.merge(pais_cumul, on=grupo_cols, how='left')
    resultado['pais_id_CUMUL'] = resultado['pais_id_CUMUL'].fillna('')

    # Selecionar apenas colunas necessárias para o resultado final
    colunas_finais = grupo_cols + ['FOB_100', 'FOB_80', 'CIF_80', 'kg_ratio', 'pais_id_CUMUL']
    resultado = resultado[colunas_finais].reset_index(drop=True)
    
    # Exportar dados dos top N% — tenta Excel, cai para CSV se falhar
    import time
    _downloads = Path.home() / "Downloads"
    _ts = datetime.now().strftime('%Y%m%d_%H%M%S')
    _base = f"top_{int(threshold * 100)}_percent_{_ts}"
    _log_path = _downloads / f"{_base}_export_log.txt"
    try:
        _excel_path = _downloads / f"{_base}.xlsx"
        resultado.to_excel(_excel_path, index=False)
        _log_path.write_text(f"OK - Excel gerado: {_excel_path}\n")
        print(f"FOI CRIADO O EXCEL VERIFIQUE: {_excel_path}")
        time.sleep(30)
    except Exception as e_xlsx:
        try:
            _csv_path = _downloads / f"{_base}.csv"
            resultado.to_csv(_csv_path, index=False)
            _log_path.write_text(f"Excel falhou ({e_xlsx}). CSV gerado: {_csv_path}\n")
            print(f"FOI CRIADO O CSV VERIFIQUE: {_csv_path}")
            time.sleep(30)
        except Exception as e_csv:
            _log_path.write_text(f"Excel falhou: {e_xlsx}\nCSV falhou: {e_csv}\n")
            print(f"[top_n_percent] ERRO ao exportar: xlsx={e_xlsx} | csv={e_csv}")
    
    resultado.drop(columns=['kg_ratio'], inplace=True)
    return resultado

def find_IDs(df, ids):
    df_ncm = ids.merge(df, right_on=['ncm', 'ImportExport'], left_on=['NCM', 'ImportExport'], how='inner')
    df_ncm.dropna(subset=['NCM'], inplace=True)
    df_ncm.drop(columns=['NCM', 'ncm', 'ImportExport'], inplace=True)

    df_ncm['IDIndicePrincipal'] = df_ncm['IDIndicePrincipal'].astype(np.int32)

    meses_por_indice = (
        df_ncm
        .drop_duplicates(subset=group_cols)  # Garante um mês único
        .groupby('IDIndicePrincipal')
        .size()
    )

    ids_com_24_meses = meses_por_indice[meses_por_indice >= 24].index
    df_ncm = df_ncm[df_ncm['IDIndicePrincipal'].isin(ids_com_24_meses)]
    
    return df_ncm

def outlier_testing_stl(series, date_column='Data', value_column='Valor', period=12, z_threshold=2.5, id_column='ID'):
    series_clean = series.set_index(date_column)

    if period % 2 == 0:
        period -= 1
    
    try:
        stl = STL(series_clean[value_column], seasonal=period, robust=True)
        result = stl.fit()
    except ValueError as e:
        print(f"Error during STL decomposition for ID {series_clean[id_column].iloc[0]}: {e}")
        return series

    series_clean["trend"] = result.trend
    series_clean["seasonality"] = result.seasonal
    series_clean["residuals"] = result.resid

    # Armazena valores utilizados para z_score
    residual_mean = series_clean["residuals"].mean()
    residual_std = series_clean["residuals"].std()

    # Valores do z_score para verificação
    series_clean["residuals_mean"] = residual_mean
    series_clean["residuals_std"] = residual_std
    series_clean["residuals_zscore"] = (series_clean["residuals"] - residual_mean) / residual_std
    # series_clean.drop(columns=["residuals_mean", "residuals_std"], inplace=True)

    # Define outliers
    outliers = pd.Series(np.abs(series_clean["residuals_zscore"]) > z_threshold, index=series_clean.index)

    series_clean["Valor_clean"] = series_clean[value_column]
    series_clean.loc[outliers, "Valor_clean"] = np.nan
    series_clean["residuals_smoothed"] = series_clean["residuals"]

    smoothed = []
    residuals = series_clean["residuals"]
    if outliers.any():
        zscores = series_clean["residuals_zscore"]
    
        for idx in range(len(series_clean)):
            if outliers.iloc[idx]:
                # Pega os resíduos anteriores que não são outliers
                valid_past_residuals = residuals.iloc[:idx][(zscores.iloc[:idx].abs() < 2.5)].dropna()
                last_3_valid = valid_past_residuals.tail(3)
            
                if len(last_3_valid) > 0:
                    smoothed.append(last_3_valid.mean())
                else:
                    smoothed.append(np.nan)
            else:
                smoothed.append(residuals.iloc[idx])
    
    if len(smoothed) > 0:
        series_clean["residuals_smoothed"] = smoothed
    else:
        series_clean["residuals_smoothed"] = residuals
    # series_clean.drop(columns=["residuals"], inplace=True)

    series_clean["Valor_final"] = series_clean["Valor_clean"].copy()
    missing_values = series_clean["Valor_clean"].isna()
    series_clean.loc[missing_values, "Valor_final"] = (
        series_clean["trend"][missing_values] +
        series_clean["seasonality"][missing_values] +
        series_clean["residuals_smoothed"][missing_values]
    )
    # series_clean.drop(columns=["Valor_clean"], inplace=True)
    
    series_clean = series_clean.reset_index()

    return series_clean

def outlier_testing_iqr(series, date_column='Data'):
    # Step 1: Detecta outliers por IQR
    series_clean = series.set_index(date_column).copy()
    series_clean['warning'] = None
     
    raw_values = series_clean.get('Valor')
    if raw_values is None:
        raise ValueError("'Valor' column is required for raw outlier detection.")

    q1_list, q3_list, iqr_list = [], [], []
    raw_values_np = raw_values.values
    
    for i in range(len(raw_values_np)):
        past_raw = raw_values_np[:i+1]
        past_raw = past_raw[~np.isnan(past_raw)]
        if len(past_raw) >= 1:
            q1 = np.percentile(past_raw, 25)
            q3 = np.percentile(past_raw, 75)
            iqr = q3 - q1
        else:
            q1 = q3 = iqr = np.nan
        q1_list.append(q1)
        q3_list.append(q3)
        iqr_list.append(iqr)

    series_clean['cumulative_Q1'] = q1_list
    series_clean['cumulative_Q3'] = q3_list
    series_clean['cumulative_IQR'] = iqr_list

    # Step 2: Deetecta outlier por IQR de acordo com dados passados
    lower_bound = series_clean['cumulative_Q1'] - 1.5 * series_clean['cumulative_IQR']
    upper_bound = series_clean['cumulative_Q3'] + 1.5 * series_clean['cumulative_IQR']
    # series_clean.drop(columns=['cumulative_Q1', 'cumulative_Q3', 'cumulative_IQR'], inplace=True)
    
    iqr_outliers = (raw_values < lower_bound) | (raw_values > upper_bound)
    
    series_clean['iqr_outlier'] = iqr_outliers
    return series_clean

def interpolation_iqr_stl(series_clean, value_column='Valor_final'):
    iqr_outliers = series_clean['iqr_outlier']
   
    # Step 2.5: Detecta sequencias de 5+ IQR outliers -- Apenas para controle futuro
    rolling_sum = iqr_outliers.rolling(window=5, min_periods=5).sum()
    outlier_mask = rolling_sum >= 5
    
    outlier_mask = outlier_mask.reindex(series_clean.index).fillna(False)
    series_clean.loc[outlier_mask.fillna(False), 'warning'] = '5+ IQR outliers in a row'
    
    # Step 3: Preserva valores 'históricos' -- Não deverão ser alterados
    is_historic = (series_clean['Type'] == 'historic')
    
    # Outliers ficarão como NaN
    series_clean.loc[iqr_outliers & ~is_historic, value_column] = np.nan

    # Interpolação de valores NaN
    series_clean[value_column] = series_clean[value_column].interpolate(method="linear")
    
    # Step 4: Corrige demais NaN com tendência + sazonalidade + resíduo suavizado
    fallback = (
        series_clean.get("trend", 0) +
        series_clean.get("seasonality", 0) +
        series_clean.get("residuals_smoothed", 0)
    )
    fill_mask = series_clean[value_column].isna() & ~is_historic
    series_clean.loc[fill_mask, value_column] = fallback[fill_mask]

    # Só executa se a última observação (Mês novo) é um outlier por IQR
    if len(series_clean) and series_clean['iqr_outlier'].iat[-1]:
        val_col  = series_clean.columns.get_loc(value_column)
        warn_col = series_clean.columns.get_loc('warning')

        # Identifica sequências de outlier por IQR
        run_start = len(series_clean) - 1
        while run_start >= 0 and series_clean['iqr_outlier'].iat[run_start]:
            run_start -= 1
        run_start += 1                         

        # Substitui do mais antigo ao mais novo com média móvel
        for i in range(run_start, len(series_clean)):
            prev_vals   = series_clean[value_column].iloc[max(0, i - 3):i]
            replacement = prev_vals.mean(skipna=True)

            if not np.isnan(replacement):
                series_clean.iat[i, val_col] = replacement
            else:
                series_clean.iat[i, warn_col] = (
                    series_clean.iat[i, warn_col] or
                    'Unable to replace trailing IQR outlier – insufficient history'
                )

    #Guarda valores históricos, para garantir que não foram substituídos.
    if 'Valor' in series_clean.columns:
        series_clean.loc[is_historic, value_column] = series_clean.loc[is_historic, 'Valor']

    return series_clean.reset_index()

def final_interpolation(series, date_column='Data', value_column='Valor_final'):
    series_clean = outlier_testing_iqr(series, date_column=date_column)
    series_clean = interpolation_iqr_stl(series_clean, value_column=value_column)
    
    # series_clean.drop(columns=['Valor', 'warning'], inplace=True)
    series_clean = optimize_dtypes(series_clean)
    return series_clean

def fix_last_month_high_residual(df, date_column='Data', value_column='Valor_final', id_column='ID'):
    """
    Ajuste do mês mais recente.

    Regra 1: Se é último mês E outlier por resíduos E por IQR -> média móvel 3 meses
    Regra 2: Se é último mês E outlier por resíduos -> tendência + sazonalidade + resíduos suavizados
    Regra 3: Se é último mês E outlier por IQR -> média móvel 3 meses
    Fix the last available month for an ID based on multiple conditions.
    """
    df_processed = df.copy()
    
    if len(df_processed) == 0:
        return df_processed
    
    df_processed = df_processed.sort_values(date_column).reset_index(drop=True)
    
    last_idx = len(df_processed) - 1
    
    required_columns_basic = ['residuals_zscore']
    missing_basic = [col for col in required_columns_basic if col not in df_processed.columns]
    
    if missing_basic:
        print(f"Warning: Missing basic columns {missing_basic} for ID {df_processed[id_column].iloc[0] if id_column in df_processed.columns else 'Unknown'}. Skipping fixes.")
        return df_processed
    
    # Calcula últimos 3 meses
    def get_last_3_average(idx):
        if idx == 0:
            return np.nan  
        prev_values = df_processed.loc[max(0, idx-3):idx-1, value_column]
        prev_values = prev_values.dropna()
        return prev_values.mean() if len(prev_values) > 0 else np.nan
    
    idx = last_idx
    
    # Não mexe no histórico
    if 'Type' in df_processed.columns and df_processed.loc[idx, 'Type'] == 'historic':
        print(f"Skipping last month fix for ID {df_processed[id_column].iloc[0] if id_column in df_processed.columns else 'Unknown'}: last value is historic")
        return df_processed
        
    residual_zscore = df_processed.loc[idx, 'residuals_zscore']
    iqr_outlier = df_processed.loc[idx, 'iqr_outlier'] if 'iqr_outlier' in df_processed.columns else False
    
    # Regra 1:
    if (pd.notna(residual_zscore) and abs(residual_zscore) > 2.5 and iqr_outlier):
        
        avg_value = get_last_3_average(idx)
        if not pd.isna(avg_value):
            df_processed.loc[idx, value_column] = avg_value
            df_processed.loc[idx, 'fix_applied'] = 'last_month_high_resid_iqr_avg3'
            print(f"Applied fix 1 for ID {df_processed[id_column].iloc[0] if id_column in df_processed.columns else 'Unknown'}: "
                  f"avg of last 3 = {avg_value:.3f}")
    
    # Regra 2:
    elif (pd.notna(residual_zscore) and abs(residual_zscore) > 2.5):
        
        trend_cols = ['trend', 'seasonality', 'residuals_smoothed']
        if all(col in df_processed.columns for col in trend_cols):
            new_value = (
                df_processed.loc[idx, 'trend'] + 
                df_processed.loc[idx, 'seasonality'] + 
                df_processed.loc[idx, 'residuals_smoothed']
            )
            df_processed.loc[idx, value_column] = new_value
            df_processed.loc[idx, 'fix_applied'] = 'last_month_high_resid_trend_seasonal'
            print(f"Applied fix 2 for ID {df_processed[id_column].iloc[0] if id_column in df_processed.columns else 'Unknown'}: "
                  f"trend+seasonal+resid_smooth = {new_value:.3f}")
        else:
            print(f"Warning: Missing trend/seasonality columns for fix 2 on ID {df_processed[id_column].iloc[0] if id_column in df_processed.columns else 'Unknown'}")
    
    # Regra 3: 
    elif iqr_outlier:
        avg_value = get_last_3_average(idx)
        if not pd.isna(avg_value):
            df_processed.loc[idx, value_column] = avg_value
            df_processed.loc[idx, 'fix_applied'] = 'last_month_iqr_outlier_avg3'
            print(f"Applied fix 3 for ID {df_processed[id_column].iloc[0] if id_column in df_processed.columns else 'Unknown'}: "
                  f"avg of last 3 = {avg_value:.3f}")
    
    else:
        df_processed.loc[idx, 'fix_applied'] = 'no_fix_needed'
    
    # df_processed.drop(columns=['residuals_smoothed'], inplace=True)
    return df_processed

def fix_negative_values(df):
    """Substitui de volta valores negativos que podem ter sido gerados em alguma das manipulações."""
    negative_mask = df["Valor_final"] < 0
    df.loc[negative_mask, "Valor_final"] = (
        df["trend"][negative_mask] + 
        df["seasonality"][negative_mask] + 
        df["residuals"][negative_mask]
    )
    # Fix NaN values (only if they exist and are not historic)
    nan_mask = df["Valor_final"].isna()
    
    # Skip historic data (if 'Type' column exists)
    if 'Type' in df.columns:
        nan_mask = nan_mask & (df['Type'] != 'historic')

    # Replace NaN values with trend + seasonality + residuals
    df.loc[nan_mask, "Valor_final"] = (
        df["trend"][nan_mask] + 
        df["seasonality"][nan_mask] + 
        df["residuals"][nan_mask]
    )
    
    # df.drop(columns=['Type', 'trend', 'seasonality', 'residuals'], inplace=True)
    return df

def find_alpha(series: pd.DataFrame):
    series['alpha'] = series['CIF_80'] / series['FOB_80']
    series = series[['IDIndicePrincipal', 'Data', 'alpha']]
    return series


class ComexPipelineNM(ABC):
    """
    Classe base abstrata para pipelines de COMEX com Nova Metodologia.
    
    Define as 4 fases do processamento: Coleta, Normalização, Cálculo e Upload.
    Utiliza a nova metodologia estatística baseada em STL e top N percent.
    """
    
    def __init__(self, iso_code: str, start_date: Optional[datetime] = None, 
                 data_contract_path: str = None, ids_table_path: str = None,
                 storage_base_path: str = "staging/comex",
                 use_azure: bool = True,
                 threshold_percent: float = 0.8,
                 min_months_required: int = 24,
                 developing: bool = False,
                 iso_database: Optional[str] = None):
        """
        Inicializa o pipeline de COMEX com Nova Metodologia.
        
        Args:
            iso_code: Código ISO do país (ex: 'ARG', 'BRA', 'CHL')
            start_date: Data inicial para coleta (padrão: 60 dias atrás)
            data_contract_path: Caminho para o arquivo data-contract.yaml
            ids_table_path: Caminho para a tabela de IDs
            storage_base_path: Caminho base no ADLS2
            use_azure: Se True, usa ADLS2; se False, usa storage local
            threshold_percent: Threshold para top_n_percent (padrão: 0.8 = 80%)
            min_months_required: Mínimo de meses necessários para filtrar IDs (padrão: 24)
            developing: Se True, não usa ADLS e salva/carrega tudo no diretório local './' (padrão: False)
            iso_database: Código ISO do país para histórico compartilhado (ex: 'EUR' para países europeus).
                         Se None, usa iso_code (padrão: None)
        """
        self.iso_code = iso_code.upper()
        self.logger = logging.getLogger(f"{self.__class__.__name__}_{self.iso_code}")
        
        # Determinar qual código usar para o histórico (compartilhado ou próprio)
        self.iso_database = iso_database.upper() if iso_database else self.iso_code
        
        # Modo desenvolvimento: força uso de storage local
        self.developing = developing
        if self.developing:
            self.logger.info("MODO DESENVOLVIMENTO ATIVADO: Usando apenas storage local (./)")
            use_azure = False
        
        # Datas
        # Padrão: 150 dias atrás (~5 meses) para garantir que o arquivo anual do ano anterior
        # seja incluído na busca quando o pipeline roda nos primeiros meses do ano.
        # Isso evita gaps no merge com o historical.parquet (ex: Dez/2025 ficava ausente
        # quando o pipeline rodava em Jan-Mar/2026 com lookback de apenas 60 dias).
        self.start_date = start_date if start_date else (datetime.now() - timedelta(days=150))
        self.start_date = self.start_date.replace(day=1)
        self.period_dates = self._generate_date_range(self.start_date)
        
        # Azure Storage
        self.use_azure = use_azure and not self.developing  # Não usar Azure em modo desenvolvimento
        self.azure_storage = None
        if self.use_azure:
            try:
                from BlobStorage_API import AzureBlobStorage
                self.azure_storage = AzureBlobStorage()
                self.logger.info("Conectado ao Azure Storage com sucesso")
            except Exception as e:
                self.logger.warning(f"Não foi possível conectar ao Azure Storage: {str(e)}")
                self.logger.warning("Usando storage local como fallback")
                self.use_azure = False
        
        # Paths
        # Usar iso_database para histórico compartilhado (ex: EUR para países europeus)
        historical_db = self.iso_database if self.iso_database != self.iso_code else self.iso_code
        
        if self.developing:
            # Modo desenvolvimento: usar diretório NM/dados/
            self.storage_base_path = "NM/dados"
            self.raw_path = f"NM/dados/{self.iso_code}/raw"
            self.silver_path = f"NM/dados/{self.iso_code}/silver"
            self.gold_path = f"NM/dados/{self.iso_code}/gold"
            # Usar iso_database para histórico compartilhado
            self.historical_data_path = f"NM/dados/{historical_db}/database"
        else:
            self.storage_base_path = storage_base_path
            self.raw_path = f"{storage_base_path}/{self.iso_code}/raw"
            self.silver_path = f"{storage_base_path}/{self.iso_code}/silver"
            self.gold_path = f"{storage_base_path}/{self.iso_code}/gold"
            # Usar iso_database para histórico compartilhado
            self.historical_data_path = f"{storage_base_path}/{historical_db}/database"
        
        # Configurações
        self.data_contract_path = data_contract_path or "data-contract.yaml"
        self.ids_table_path = ids_table_path or "IDS_comex.xlsx"
        
        # Parâmetros da nova metodologia
        self.threshold_percent = threshold_percent
        self.min_months_required = min_months_required
        
        # DataFrames de trabalho
        self.raw_import_df = None
        self.raw_export_df = None
        self.raw_df = None  # Dados brutos (sem normalização do data-contract)
        self.historical_df = None  # DataFrame histórico completo do ADLS (usado quando não há dados novos)
        self.gold_df = None
        
        self.logger.info(f"Pipeline NM inicializado para {self.iso_code}")
        if self.iso_database != self.iso_code:
            self.logger.info(f"Histórico compartilhado: usando database de {self.iso_database}")
        self.logger.info(f"Período: {self.start_date.strftime('%Y-%m-%d')} até {datetime.now().strftime('%Y-%m-%d')}")
        if self.developing:
            self.logger.info(f"Storage: Local (MODO DESENVOLVIMENTO - NM/dados/{self.iso_code}/)")
        else:
            self.logger.info(f"Storage: {'Azure ADLS2' if self.use_azure else 'Local'}")
        self.logger.info(f"Threshold: {self.threshold_percent}, Min meses: {self.min_months_required}")
    
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
            current_file = Path(__file__).resolve()
            current_path_str = str(current_file)
            
            # Usar variável de ambiente se disponível
            airflow_include_path = os.getenv('AIRFLOW_INCLUDE_PATH', '/opt/airflow/include')
            if airflow_include_path in current_path_str:
                return False
            
            if os.environ.get('AIRFLOW_HOME') or os.environ.get('AIRFLOW_CONFIG'):
                return False
            
            return True
        except Exception:
            return True
    
    def _get_cache_path(self) -> Path:
        """Retorna o caminho do diretório de cache."""
        if self.developing:
            # Modo desenvolvimento: usar NM/dados/{iso_code}/cache
            cache_dir = Path(f"NM/dados/{self.iso_code}/cache")
        else:
            # Modo produção: usar .cache/comex/{iso_code}
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
            file_mtime = datetime.fromtimestamp(cache_file.stat().st_mtime)
            age = datetime.now() - file_mtime
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
    
    def collect_import_data(self) -> pd.DataFrame:
        """
        Método para coletar dados de importação.
        Deve ser implementado por cada país específico quando iso_database != 'EUR'.
        Para países que usam iso_database == 'EUR', retorna DataFrame vazio.
        
        Returns:
            DataFrame com dados brutos de importação
        """
        if self.iso_database == 'EUR':
            self.logger.info("iso_database == 'EUR': collect_import_data não é necessário")
            return pd.DataFrame()
        else:
            raise NotImplementedError(
                f"collect_import_data deve ser implementado para {self.iso_code} "
                f"(iso_database != 'EUR')"
            )
    
    def collect_export_data(self) -> pd.DataFrame:
        """
        Método para coletar dados de exportação.
        Deve ser implementado por cada país específico quando iso_database != 'EUR'.
        Para países que usam iso_database == 'EUR', retorna DataFrame vazio.
        
        Returns:
            DataFrame com dados brutos de exportação
        """
        if self.iso_database == 'EUR':
            self.logger.info("iso_database == 'EUR': collect_export_data não é necessário")
            return pd.DataFrame()
        else:
            raise NotImplementedError(
                f"collect_export_data deve ser implementado para {self.iso_code} "
                f"(iso_database != 'EUR')"
            )
    
    def collect(self) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Executa a fase de coleta completa.
        Usa cache local se disponível e válido (apenas em ambiente de desenvolvimento).
        
        Quando iso_database == 'EUR', pula a coleta pois os dados vêm do histórico compartilhado.
        
        Returns:
            Tupla (import_df, export_df)
        """
        self.logger.info("=== INICIANDO FASE 1: COLETA ===")
        
        # Se iso_database == 'EUR', pular coleta (dados vêm do histórico compartilhado)
        if self.iso_database == 'EUR':
            self.logger.info(f"iso_database == 'EUR' detectado. Pulando coleta - dados serão carregados do histórico compartilhado.")
            self.logger.info("=== FASE 1 CONCLUÍDA (pulada - usando histórico EUR) ===\n")
            return
        
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
        
        # Criar raw_df concatenando import e export
        self.logger.info("Concatenando dados brutos de importação e exportação...")
        dfs_to_concat = []
        if not self.raw_import_df.empty:
            dfs_to_concat.append(self.raw_import_df)
        if not self.raw_export_df.empty:
            dfs_to_concat.append(self.raw_export_df)
        
        if dfs_to_concat:
            self.raw_df = pd.concat(dfs_to_concat, ignore_index=True)
            self.logger.info(f"Dados brutos concatenados: {len(self.raw_df)} registros totais")
        else:
            self.raw_df = pd.DataFrame()
            self.logger.warning("Nenhum dado para concatenar")
        
        self.logger.info("=== FASE 1 CONCLUÍDA ===\n")
        return self.raw_import_df, self.raw_export_df
    
    # ============= FASE 2: NORMALIZAÇÃO =============
    
    def load_data_contract(self) -> Dict:
        """Carrega o arquivo data-contract.yaml com mapeamento de colunas."""
        contract_path = Path(self.data_contract_path)
        
        # Tentar múltiplos caminhos possíveis (em ordem de prioridade)
        possible_paths = []
        
        # 1. Path relativo ao diretório serie_temporal (preferido)
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
            else:
                self.logger.debug(f"  Data contract não encontrado em {path_name}: {path.absolute()}")
        
        self.logger.error(f"Data contract não encontrado em nenhum dos caminhos tentados")
        self.logger.warning("Continuando sem data contract - normalização de colunas será ignorada")
        return {}
    
    def load_ids_table(self) -> pd.DataFrame:
        """
        Carrega a tabela de IDs do arquivo Excel.
        
        Lê o arquivo IDS_comex.xlsx com colunas: IDIndicePrincipal, NCM, ImportExport, Pais_1.
        Filtra por país usando _get_country_name() e apenas IDs >= 382958 (nova metodologia).
        
        Returns:
            DataFrame com colunas: NCM, ImportExport, IDIndicePrincipal
        """
        try:
            # Usar ids_table_path se fornecido, caso contrário usar nome padrão
            filename = self.ids_table_path if self.ids_table_path else 'IDS_comex.xlsx'
            
            # Tentar múltiplos caminhos possíveis (em ordem de prioridade)
            possible_paths = []
            
            # 1. Diretório library (preferido - onde o arquivo realmente está)
            library_path = Path(f"library/IDS_comex.xlsx")
            possible_paths.append(("diretório library", library_path))
            
            # 2. Path especificado pelo usuário (relativo ou absoluto)
            user_path = Path(filename)
            if user_path.is_absolute():
                possible_paths.append(("path especificado (absoluto)", user_path))
            else:
                # Se relativo, tentar a partir da raiz do projeto
                possible_paths.append(("path especificado (raiz)", user_path))
            
            # 3. Paths alternativos comuns
            possible_paths.append(("diretório include", Path("include") / filename))
            possible_paths.append(("diretório config", Path("config") / filename))
            possible_paths.append(("diretório data", Path("data") / filename))
            
            # Tentar cada path até encontrar o arquivo
            ids_mapping = None
            used_path = None
            for path_name, path in possible_paths:
                if path.exists():
                    try:
                        ids_mapping = pd.read_excel(path)
                        used_path = path
                        self.logger.info(f"✓ Tabela de IDs carregada de {path_name}: {path.absolute()}")
                        break
                    except Exception as e:
                        self.logger.warning(f"Erro ao carregar IDs de {path_name} ({path}): {str(e)}")
                        continue
                else:
                    self.logger.debug(f"  Tabela de IDs não encontrada em {path_name}: {path.absolute()}")
            
            # Se nenhum path funcionou, levantar erro
            if ids_mapping is None:
                error_msg = f"Arquivo {filename} não encontrado em nenhum dos caminhos tentados"
                self.logger.error(error_msg)
                raise FileNotFoundError(error_msg)
            
            # Ler Excel (já carregado acima)
            
            # Verificar se colunas esperadas existem
            expected_columns = ['IDIndicePrincipal', 'NCM', 'ImportExport', 'Pais_1']
            missing_columns = [col for col in expected_columns if col not in ids_mapping.columns]
            if missing_columns:
                self.logger.error(f"Colunas esperadas não encontradas no arquivo: {missing_columns}")
                self.logger.error(f"Colunas disponíveis: {list(ids_mapping.columns)}")
                raise ValueError(f"Colunas obrigatórias ausentes: {missing_columns}")
            
            # Obter nome do país para filtro
            country_name = self._get_country_name()
            self.logger.info(f"Filtrando IDs para país: {country_name}")
            
            # Filtrar por país
            ids_mapping = ids_mapping[ids_mapping['Pais_1'] == country_name].copy()
            
            # Brasil tem OM e NM precisamos filtrar apenas NM
            if country_name == 'Brasil':
                # Filtrar apenas IDs com nova metodologia (IDIndicePrincipal >= 382958)
                nova_metodologia = 382958
                ids_mapping = ids_mapping[ids_mapping['IDIndicePrincipal'] >= nova_metodologia]
            
            if ids_mapping.empty:
                self.logger.warning(f"Nenhum ID encontrado para país '{country_name}'")
                return pd.DataFrame(columns=['NCM', 'ImportExport', 'IDIndicePrincipal'])
            
            # Converter tipos de dados
            ids_mapping['NCM'] = pd.to_numeric(ids_mapping['NCM'], errors='coerce').astype('Int64')
            ids_mapping['ImportExport'] = pd.to_numeric(ids_mapping['ImportExport'], errors='coerce').astype('Int64')
            ids_mapping['IDIndicePrincipal'] = pd.to_numeric(ids_mapping['IDIndicePrincipal'], errors='coerce').astype('Int64')
            
            # Remover linhas com valores nulos após conversão
            ids_mapping = ids_mapping.dropna(subset=['NCM', 'ImportExport', 'IDIndicePrincipal'])
            
            if self.iso_database != 'EUR':
                # Filtrar apenas IDs >= 382958 (nova metodologia)
                nova_metodologia = 382958
                ids_mapping = ids_mapping[ids_mapping['IDIndicePrincipal'] >= nova_metodologia]
            
            # Selecionar apenas colunas necessárias
            ids_mapping = ids_mapping[['NCM', 'ImportExport', 'IDIndicePrincipal']].copy()
            ids_mapping.reset_index(drop=True, inplace=True)
            
            self.logger.info(f"IDs carregados: {len(ids_mapping)} registros para {country_name})")
            return ids_mapping
            
        except Exception as e:
            self.logger.error(f"Erro ao carregar IDs: {str(e)}")
            raise
    
    @abstractmethod
    def _get_country_name(self) -> str:
        """Retorna o nome do país usado na tabela de IDs."""
        pass
    
    def _get_country_code(self) -> str:
        """
        Retorna o código do país usado para filtrar dados do EUR.
        Por padrão, tenta mapear iso_code para código de 2 letras.
        Pode ser sobrescrito por classes específicas se necessário.
        
        Returns:
            Código do país (ex: 'NL', 'DE', 'FR')
        """
        # Mapeamento comum de códigos ISO de 3 letras para 2 letras
        iso_to_code = {
            'NLD': 'NL',  # Holanda
            'DEU': 'DE',  # Alemanha
            'FRA': 'FR',  # França
            'ITA': 'IT',  # Itália
            'ESP': 'ES',  # Espanha
            'BEL': 'BE',  # Bélgica
            'PRT': 'PT',  # Portugal
            'AUT': 'AT',  # Áustria
            'SWE': 'SE',  # Suécia
            'DNK': 'DK',  # Dinamarca
            'FIN': 'FI',  # Finlândia
            'POL': 'PL',  # Polônia
            'CZE': 'CZ',  # República Tcheca
            'HUN': 'HU',  # Hungria
            'GRC': 'GR',  # Grécia
            'IRL': 'IE',  # Irlanda
            'ROU': 'RO',  # Romênia
            'BGR': 'BG',  # Bulgária
        }
        
        # Se houver mapeamento específico, usar
        if self.iso_code in iso_to_code:
            return iso_to_code[self.iso_code]
        
        # Caso contrário, tentar usar últimos 2 caracteres
        if len(self.iso_code) >= 2:
            return self.iso_code[-2:]
        
        return self.iso_code
    
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
        
        country_section = contract.get(self.iso_code)
        if not country_section:
            self.logger.warning(f"Seção {self.iso_code} não encontrada no data contract")
            return df
        
        ie_type = 'import' if import_export == 1 else 'export'
        type_mapping = country_section.get(ie_type)
        
        if not type_mapping:
            self.logger.warning(f"Mapeamento para {ie_type} não encontrado no data contract")
            return df
        
        mapping = {}
        for key, value in type_mapping.items():
            if key != 'columns':
                mapping[key] = value
        
        if not mapping:
            self.logger.warning(f"Nenhum mapeamento encontrado para {ie_type}")
            return df
        
        df_normalized = df.rename(columns=mapping)
        self.logger.info(f"Colunas normalizadas ({ie_type}): {list(mapping.keys())} -> {list(mapping.values())}")
        
        if 'data' in df_normalized.columns and 'Data' not in df_normalized.columns:
            df_normalized.rename(columns={'data': 'Data'}, inplace=True)
        
        expected_columns = type_mapping.get('columns', [])
        if expected_columns:
            available_cols = [col for col in expected_columns if col in df_normalized.columns]
            if available_cols:
                df_normalized = df_normalized[available_cols]
                self.logger.info(f"Colunas filtradas para: {available_cols}")
            else:
                self.logger.warning(f"Nenhuma das colunas esperadas encontrada: {expected_columns}")
        
        return df_normalized
    
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
        
        for column, dtype_str in schema.items():
            if column not in df.columns:
                continue
            
            dtype_str_lower = dtype_str.lower()
            
            if dtype_str_lower == 'datetime':
                df[column] = pd.to_datetime(df[column], errors='coerce')
            elif dtype_str_lower == 'int':
                if df[column].dtype == 'object':
                    df[column] = df[column].astype(str).str.replace(',', '.')
                df[column] = pd.to_numeric(df[column], errors='coerce')
                df.dropna(subset=[column], inplace=True)
            elif dtype_str_lower == 'float':
                if df[column].dtype == 'object':
                    df[column] = df[column].astype(str).str.replace(',', '.')
                df[column] = pd.to_numeric(df[column], errors='coerce')
            elif dtype_str_lower == 'str':
                df[column] = df[column].astype(str)
        
        if 'ImportExport' in df.columns:
            df['ImportExport'] = df['ImportExport'].astype(int)
        
        return df
    
    def _standardize_data_types_fallback(self, df: pd.DataFrame) -> pd.DataFrame:
        """Fallback para padronização de tipos quando schema não está disponível."""
        if 'peso' in df.columns:
            df['peso'] = df['peso'].astype(str).str.replace(',', '.')
            df['peso'] = pd.to_numeric(df['peso'], errors='coerce')
        
        if 'valor' in df.columns:
            df['valor'] = df['valor'].astype(str).str.replace(',', '.')
            df['valor'] = pd.to_numeric(df['valor'], errors='coerce')
        
        if 'ncm' in df.columns:
            df['ncm'] = pd.to_numeric(df['ncm'], errors='coerce')
            df.dropna(subset=['ncm'], inplace=True)
            df['ncm'] = df['ncm'].astype(int)
        
        if 'ImportExport' in df.columns:
            df['ImportExport'] = df['ImportExport'].astype(int)
        
        if 'Data' in df.columns:
            df['Data'] = pd.to_datetime(df['Data'])
        
        return df
    
    # ============= NORMALIZAÇÃO PARA SCHEMA DO HISTÓRICO =============
    
    def _update_historical_data(self, existing_historical: pd.DataFrame, 
                                new_data: pd.DataFrame, 
                                update_months: int = 3) -> pd.DataFrame:
        """
        Atualiza histórico substituindo os últimos N meses pelos novos dados.
        
        Args:
            existing_historical: DataFrame com histórico existente
            new_data: DataFrame com novos dados no schema do histórico
            update_months: Número de meses para substituir (padrão: 3)
            
        Returns:
            DataFrame com histórico atualizado
        """
        if existing_historical.empty:
            self.logger.info("Histórico vazio, usando apenas novos dados")
            return new_data.copy()
        
        if new_data.empty:
            self.logger.info("Nenhum dado novo, retornando histórico existente")
            return existing_historical.copy()
        
        # Criar coluna Data auxiliar para comparação
        existing_historical = existing_historical.copy()
        new_data = new_data.copy()
        
        # Criar Data a partir de CO_ANO e CO_MES
        existing_historical['Data'] = pd.to_datetime(
            existing_historical['CO_ANO'].astype(str) + '-' + 
            existing_historical['CO_MES'].astype(str).str.zfill(2) + '-01'
        )
        
        new_data['Data'] = pd.to_datetime(
            new_data['CO_ANO'].astype(str) + '-' + 
            new_data['CO_MES'].astype(str).str.zfill(2) + '-01'
        )
        
        # Normalizar datas para primeiro dia do mês para comparação
        existing_historical['Data_month'] = existing_historical['Data'].dt.to_period('M').dt.to_timestamp()
        new_data['Data_month'] = new_data['Data'].dt.to_period('M').dt.to_timestamp()
        
        # Calcular data de corte: últimos N meses a partir de hoje
        cutoff_date = existing_historical.Data.max() - pd.DateOffset(months=update_months)
        cutoff_date = cutoff_date.replace(day=1)
        
        self.logger.info(f"Substituindo meses a partir de {cutoff_date.strftime('%Y-%m-%d')} (últimos {update_months} meses)")
        
        # Remover últimos N meses do histórico existente
        historical_old = existing_historical[existing_historical['Data_month'] <= cutoff_date].copy()
        
        # Remover coluna auxiliar Data_month
        if 'Data_month' in historical_old.columns:
            historical_old = historical_old.drop(columns=['Data_month', 'Data'])
        
        self.logger.info(f"Histórico antigo mantido: {len(historical_old)} registros (antes de {cutoff_date.strftime('%Y-%m-%d')})")
        
        # Filtrar novos dados para os meses que serão substituídos
        new_data_filtered = new_data[new_data['Data_month'] > cutoff_date].copy()
        
        # Remover coluna auxiliar Data_month
        if 'Data_month' in new_data_filtered.columns:
            new_data_filtered = new_data_filtered.drop(columns=['Data_month', 'Data'])
        
        self.logger.info(f"Dados novos para substituir: {len(new_data_filtered)} registros")
        
        # Se não houver dados novos, retornar histórico existente sem atualizar
        if new_data_filtered.empty:
            self.logger.info("Nenhum dado novo para substituir, mantendo histórico existente sem atualização")
            # Remover colunas auxiliares do histórico existente antes de retornar
            existing_historical_clean = existing_historical.copy()
            if 'Data_month' in existing_historical_clean.columns:
                existing_historical_clean = existing_historical_clean.drop(columns=['Data_month', 'Data'])
            return existing_historical_clean
        
        # Combinar histórico antigo (meses preservados) com novos dados (meses substituídos)
        combined = pd.concat([historical_old, new_data_filtered], ignore_index=True)

        if 'Data' not in combined.columns:
            contract = self.load_data_contract()
            combined = self._country_specific_treatment(combined)

            # separar entre import e export pois pode haver colunas diferentes, vide data-contract.yaml
            combined_export = combined[combined['ImportExport'] == 0].copy()
            combined_import = combined[combined['ImportExport'] == 1].copy()

            # Normalizar historico usando data-contract
            combined_export = self.normalize_columns(combined_export, contract, import_export=1)
            combined_import = self.normalize_columns(combined_import, contract, import_export=0)

            combined_normalized = pd.concat([combined_export, combined_import], ignore_index=True)
            combined = combined_normalized.copy()
        
        # Ordenar por Data, ncm, pais_id, ImportExport
        key_columns = ['Data', 'ncm', 'pais_id', 'ImportExport']
        combined = combined.sort_values(by=key_columns)
        
        self.logger.info(f"Histórico atualizado: {len(combined)} registros total "
                        f"({len(historical_old)} antigos preservados + {len(new_data_filtered)} novos substituídos)")
        
        return combined
    
    def _raw_to_historical_schema(self, df: pd.DataFrame, existing_historical: pd.DataFrame != None) -> pd.DataFrame:
        """
        Converte dados brutos (raw_df) diretamente para schema do histórico.
        
        Esta função mapeia colunas brutas (que variam por país) para o schema do histórico
        usando o histórico existente para obter os nomes e tipos das colunas dinamicamente.
        É usada apenas em update_historical() para converter dados brutos antes de atualizar o histórico.
        
        Args:
            df: DataFrame com dados brutos (raw_df)
            existing_historical: DataFrame com histórico existente (obrigatório).
            
        Returns:
            DataFrame no schema do histórico
        """
        if existing_historical is None or existing_historical.empty:
            self.logger.error("existing_historical não pode ser None ou vazio")
            return pd.DataFrame()

        # Obter colunas e tipos do histórico existente
        historical_columns = list(existing_historical.columns)
        historical_dtypes = existing_historical.dtypes.to_dict()
        self.logger.info(f"Schema obtido do histórico: {len(historical_columns)} colunas")
        self.logger.debug(f"Colunas do histórico: {historical_columns}")
        
        df_result = df.copy()
        df_result = df_result[historical_columns]

        # Aplicar tipos do histórico
        for col, dtype in historical_dtypes.items():
            if col in df_result.columns:
                try:
                    # Converter tipos numéricos
                    if pd.api.types.is_integer_dtype(dtype) or str(dtype) in ['int64', 'int32', 'Int64']:
                        df_result[col] = pd.to_numeric(df_result[col], errors='coerce')
                        if str(dtype) == 'Int64':
                            df_result[col] = df_result[col].astype('Int64')
                        else:
                            df_result[col] = df_result[col].astype(int)
                    elif pd.api.types.is_float_dtype(dtype) or str(dtype) in ['float64', 'float32']:
                        df_result[col] = pd.to_numeric(df_result[col], errors='coerce').astype(float)
                    elif pd.api.types.is_string_dtype(dtype) or str(dtype) == 'object':
                        df_result[col] = df_result[col].astype(str)
                except Exception as e:
                    self.logger.warning(f"Erro ao converter tipo de {col} para {dtype}: {str(e)}")
        
        self.logger.info(f"Dados brutos convertidos para schema histórico: {len(df_result)} registros")
        
        return df_result
    
    def update_historical(self, update_months: int = 3):
        """
        Carrega histórico existente, atualiza com novos dados coletados e salva no ADLS.
        
        Substitui os últimos N meses do histórico pelos dados mais recentes coletados.
        
        Quando iso_database == 'EUR', apenas carrega e filtra o histórico pelo país,
        sem tentar atualizar (pois não há dados coletados).
        
        Args:
            update_months: Número de meses para substituir (padrão: 3)
        """
        self.logger.info("=== INICIANDO ATUALIZAÇÃO DO HISTÓRICO ===")
        
        # Carregar histórico existente uma única vez (será usado para schema e atualização)
        existing_historical = self._load_historical_data()
        
        # checa se historico vazio
        if existing_historical.empty:
            self.logger.info("=== HISTÓRICO VAZIO ===\n")
            return
        
        # Se iso_database == 'EUR', apenas carregar e filtrar histórico
        if self.iso_database == 'EUR':
            self.historical_df = existing_historical.copy()
            
            if self.historical_df.empty:
                self.logger.warning("Nenhum dado encontrado após filtrar histórico por país")
            
            self.logger.info("=== ATUALIZAÇÃO DO HISTÓRICO CONCLUÍDA (EUR) ===\n")
            return
                
        # Verificar se temos dados coletados
        if self.raw_df is None or self.raw_df.empty:
            self.logger.warning("raw_df está vazio, usando histórico existente sem atualização")
            self.historical_df = existing_historical.copy()
            self.logger.info("=== ATUALIZAÇÃO DO HISTÓRICO CONCLUÍDA (sem dados novos) ===\n")
            return
        
        # Converter raw_df para schema do histórico
        self.logger.info("Convertendo raw_df para schema do histórico...")
        raw_df_for_historical = self._raw_to_historical_schema(self.raw_df, existing_historical=existing_historical)
        
        if raw_df_for_historical.empty:
            self.logger.error("Não foi possível converter raw_df para schema do histórico")
            # Usar histórico existente já carregado como fallback
            self.historical_df = existing_historical.copy()
            self.logger.info("=== ATUALIZAÇÃO DO HISTÓRICO CONCLUÍDA (erro na conversão) ===\n")
            return
        
        # Atualizar histórico substituindo últimos N meses
        self.logger.info(f"Atualizando histórico substituindo últimos {update_months} meses...")
        updated_historical = self._update_historical_data(
            existing_historical, 
            raw_df_for_historical, 
            update_months=update_months
        )
               
        # Validação: proteger histórico existente de ser substituído por um menor
        existing_size = len(existing_historical)
        updated_size = len(updated_historical)
        
        # Se o histórico atualizado for menor que o existente, não substituir
        if updated_size < existing_size:
            self.logger.warning(f"Histórico atualizado ({updated_size} registros) é menor que o existente "
                                f"({existing_size} registros). Histórico não será substituído para evitar perda de dados.")
            self.logger.info("Usando dados históricos existentes para cálculos")
            self.historical_df = existing_historical.copy()
            self.logger.info("=== ATUALIZAÇÃO DO HISTÓRICO CONCLUÍDA (protegido) ===\n")
            return
        
        # Se o histórico existente for maior ou igual ao raw_df e o atualizado for menor ou igual ao existente
        if updated_size == existing_size:
            self.logger.warning(f"Histórico atualizado ({updated_size} registros) é  igual ao historico antigo ")
            self.logger.info("Usando dados históricos existentes para cálculos")
            self.historical_df = existing_historical.copy()
            self.logger.info("=== ATUALIZAÇÃO DO HISTÓRICO CONCLUÍDA (protegido) ===\n")
            return
        
        updated_historical = optimize_dtypes(updated_historical)
        updated_historical.reset_index(drop=True, inplace=True)

        # Salvar histórico atualizado no ADLS (usando iso_database para caminho)
        if not updated_historical.empty:
            self.logger.info(f"Salvando histórico atualizado no ADLS (database: {self.iso_database})...")
            self._save_to_storage(updated_historical, self.historical_data_path, "historical.parquet")
            self.logger.info(f"✓ Histórico atualizado salvo: {len(updated_historical)} registros em {self.iso_database}/database")
        else:
            self.logger.warning("Histórico atualizado está vazio, não foi salvo")
        
        self.logger.info("=== ATUALIZAÇÃO DO HISTÓRICO CONCLUÍDA ===\n")

        self.historical_df = updated_historical.copy()
    
    # ============= NORMALIZAÇÃO DO HISTÓRICO =============
    
    def normalize_historical(self):
        """
        Normaliza o histórico (self.historical_df) usando data-contract.yaml antes dos cálculos.
        
        Este método deve ser executado após update_historical() e antes de calculate().
        Normaliza os nomes das colunas do histórico usando o mapeamento do data-contract.yaml,
        similar ao processo de normalização dos dados coletados.
        """
        self.logger.info("=== INICIANDO NORMALIZAÇÃO DO HISTÓRICO ===")
        
        # Verificar se temos histórico para normalizar
        if self.historical_df is None or self.historical_df.empty:
            self.logger.warning("historical_df está vazio, não há dados para normalizar")
            self.logger.info("=== NORMALIZAÇÃO DO HISTÓRICO CONCLUÍDA (sem dados) ===\n")
            return
        
        self.logger.info(f"Normalizando histórico: {len(self.historical_df)} registros")
        
        # Carregar data contract
        contract = self.load_data_contract()
        
        if not contract:
            self.logger.warning("Data contract não encontrado")
            self.logger.info("=== NORMALIZAÇÃO DO HISTÓRICO NÃO REALIZADA ===\n")
            return
        
        historical_treated = self._country_specific_treatment(self.historical_df)

        # separar entre import e export pois pode haver colunas diferentes, vide data-contract.yaml
        historical_export = historical_treated[historical_treated['ImportExport'] == 0].copy()
        historical_import = historical_treated[historical_treated['ImportExport'] == 1].copy()

        # Normalizar historico usando data-contract
        historical_export = self.normalize_columns(historical_export, contract, import_export=0)
        historical_import = self.normalize_columns(historical_import, contract, import_export=1)

        historical_normalized = pd.concat([historical_export, historical_import], ignore_index=True)
        self.historical_df = historical_normalized.copy()
        
        self.logger.info("=== NORMALIZAÇÃO DO HISTÓRICO CONCLUÍDA ===\n")
    
    # ============= FASE 3: CÁLCULO (NOVA METODOLOGIA) =============
    
    def _load_historical_data(self) -> pd.DataFrame:
        """
        Carrega dados históricos de 5 anos do ADLS (se disponível).
        
        Quando developing=True, carrega diretamente do caminho local usando os.getcwd() + path.
        Usa iso_database para determinar qual histórico carregar (compartilhado ou próprio).
        
        Returns:
            DataFrame com dados históricos ou DataFrame vazio
        """
        try:
            # Log informativo sobre qual database está sendo usado
            if self.iso_database != self.iso_code:
                self.logger.info(f"Carregando histórico compartilhado de {self.iso_database} (país: {self.iso_code})")
            
            # Modo desenvolvimento: carregar diretamente do caminho local
            if self.developing:
                # Estratégia de busca: tentar múltiplos paths possíveis
                possible_paths = []
                
                # 1. Primeiro tentar no novo diretório de dados: NM/dados/{ISO_CODE}/database/
                country_specific_path = Path(f"NM/dados/{self.iso_code}/database/historical.parquet")
                possible_paths.append(("diretório de dados do país", country_specific_path))
                
                # 2. Se iso_database != iso_code, tentar no diretório compartilhado: NM/dados/{ISO_DATABASE}/database/
                if self.iso_database != self.iso_code:
                    shared_path = Path(f"NM/dados/{self.iso_database}/database/historical.parquet")
                    possible_paths.append((f"diretorio compartilhado ({self.iso_database})", shared_path))
                
                # 3. Fallback: tentar path antigo (serie_temporal) para compatibilidade
                old_country_path = Path(f"NM/serie_temporal/COMEX_{self.iso_code}/{self.iso_code}/database/historical.parquet")
                possible_paths.append(("diretório antigo (serie_temporal)", old_country_path))
                
                if self.iso_database != self.iso_code:
                    old_shared_path = Path(f"NM/serie_temporal/COMEX_{self.iso_database}/{self.iso_database}/database/historical.parquet")
                    possible_paths.append((f"diretorio antigo compartilhado ({self.iso_database})", old_shared_path))
                
                # 3. Fallback: tentar path antigo (raiz do projeto)
                fallback_path = Path(self.historical_data_path.lstrip('./')) / "historical.parquet"
                possible_paths.append(("path antigo (raiz)", fallback_path))
                
                # Tentar cada path até encontrar o arquivo
                for path_name, local_path in possible_paths:
                    self.logger.info(f"Tentando carregar histórico do {path_name}: {local_path.absolute()}")
                    
                    if local_path.exists():
                        df_historical = pd.read_parquet(local_path)
                        self.logger.info(f"✓ Dados históricos carregados de {path_name}: {len(df_historical)} registros")
                        return df_historical
                    else:
                        self.logger.debug(f"  Arquivo não encontrado em: {local_path.absolute()}")
                
                # Se nenhum path funcionou, avisar e retornar DataFrame vazio
                self.logger.warning(f"Arquivo histórico não encontrado em nenhum dos paths tentados")
                return pd.DataFrame()
            else:
                # Modo produção: usar método padrão de storage
                df_historical = self._load_from_storage(self.historical_data_path, "historical.parquet")
                
                if not df_historical.empty:
                    self.logger.info(f"Dados históricos carregados: {len(df_historical)} registros")
                    return df_historical
                else:
                    self.logger.warning("Nenhum dado histórico encontrado")
                    return pd.DataFrame()
        except Exception as e:
            self.logger.warning(f"Erro ao carregar dados históricos: {str(e)}")
            return pd.DataFrame()
    
    def calculate(self) -> pd.DataFrame:
        """
        Executa a fase de cálculo usando a nova metodologia.
        
        Pipeline completo:
        1. Preparação de dados e validação
        2. Encontrar IDs válidos (find_IDs)
        3. Top N Percent (top_n_percent)
        4. Preparar séries temporais
        5. Calcular alpha (razão CIF/FOB)
        6. Pipeline de processamento FOB (STL, interpolação, correções)
        7. Calcular Valor_Cif
        
        Returns:
            DataFrame final com colunas: ID, Data, Valor, Valor_Cif
        """
        self.logger.info("=== INICIANDO FASE 3: CÁLCULO (NOVA METODOLOGIA) ===")
        
        # Limpeza de memória: deletar variáveis não utilizadas nos cálculos seguintes
        self.logger.info("Limpando memória de variáveis não utilizadas...")
        if hasattr(self, 'raw_import_df') and self.raw_import_df is not None:
            del self.raw_import_df
            self.logger.debug("raw_import_df deletado da memória")
        
        if hasattr(self, 'raw_export_df') and self.raw_export_df is not None:
            del self.raw_export_df
            self.logger.debug("raw_export_df deletado da memória")
        
        if hasattr(self, 'raw_df') and self.raw_df is not None:
            del self.raw_df
            self.logger.debug("raw_df deletado da memória")
        
        # Forçar coleta de lixo do Python
        gc.collect()
        self.logger.info("Limpeza de memória concluída")
        
        # Verificar se histórico foi normalizado
        if self.historical_df is None or self.historical_df.empty:
            self.logger.error("historical_df está vazio, não é possível calcular")
            return pd.DataFrame()
        
        df_for_calculation = self.historical_df.copy()
        self.logger.info(f"Dados históricos normalizados carregados: {len(df_for_calculation)} registros")
        
        # verifica se ha coluna com separacao de paises
        if not 'pais_id' in df_for_calculation:
            if 'pais_name' in df_for_calculation:
                df_for_calculation.rename(columns={'pais_name': 'pais_id'}, inplace=True)

        # Verificar se todas as colunas necessárias existem (já devem estar normalizadas)
        required_cols = ['ncm', 'Data', 'pais_id', 'valor', 'frete', 'seguro', 'peso', 'ImportExport']
        missing_cols = [col for col in required_cols if col not in df_for_calculation.columns]
        if missing_cols:
            self.logger.error(f"Colunas obrigatórias ausentes: {missing_cols}")
            self.logger.error(f"Colunas disponíveis: {list(df_for_calculation.columns)}")
            self.logger.error("Certifique-se de que normalize_historical() foi executado antes de calculate()")
            return pd.DataFrame()
        
        # Carregar tabela de IDs
        try:
            ids_table = self.load_ids_table()
            # Preparar IDs para merge
            ids_table['NCM'] = ids_table['NCM'].astype(str)
            ids_table['NCM'] = pd.to_numeric(ids_table['NCM'], errors='coerce').astype('Int64')
            ids_table = ids_table[['NCM', 'ImportExport', 'IDIndicePrincipal']].copy()
            self.logger.info(f"Tabela de IDs carregada: {len(ids_table)} registros")
        except Exception as e:
            self.logger.error(f"Erro ao carregar tabela de IDs: {str(e)}")
            return pd.DataFrame()
        
        # Encontrar IDs válidos (find_IDs)
        self.logger.info("Encontrando IDs válidos...")
        try:
            df_ncm = find_IDs(df_for_calculation, ids_table)
            self.logger.info(f"IDs válidos encontrados: {df_ncm['IDIndicePrincipal'].nunique()} IDs únicos")
        except Exception as e:
            self.logger.error(f"Erro ao executar find_IDs: {str(e)}")
            return pd.DataFrame()
        
        if df_ncm.empty:
            self.logger.warning("Nenhum ID válido encontrado após find_IDs")
            return pd.DataFrame()
        
        # Top N Percent (top_n_percent)
        self.logger.info(f"Aplicando top_n_percent com threshold {self.threshold_percent}...")
        try:
            series = top_n_percent(df_ncm, threshold=self.threshold_percent)
            self.logger.info(f"Séries após top_n_percent: {series['IDIndicePrincipal'].nunique()} IDs únicos")
        except Exception as e:
            self.logger.error(f"Erro ao executar top_n_percent: {str(e)}")
            return pd.DataFrame()
        
        if series.empty:
            self.logger.warning("Nenhuma série gerada após top_n_percent")
            return pd.DataFrame()
        
        # Preparar séries temporais
        self.logger.info("Preparando séries temporais...")
        # series['Data'] = pd.to_datetime(
        #     series['CO_ANO'].astype(str) + '-' + 
        #     series['CO_MES'].astype(str).str.zfill(2) + '-01'
        # )
        
        # Marcar Type: 'historic' para dados antigos, 'target' para dados >= 2022-01-01
        series['Type'] = 'historic'
        series.loc[series['Data'] >= '2022-01-01', 'Type'] = 'target'
        
        # Calcular alpha (razão CIF/FOB)
        self.logger.info("Calculando alpha (razão CIF/FOB)...")
        try:
            series_for_alpha = series[series['CIF_80'] > 0].copy()
            if not series_for_alpha.empty:
                alpha = series_for_alpha.groupby(['IDIndicePrincipal'], as_index=False).apply(find_alpha)
                alpha.reset_index(drop=True, inplace=True)
                self.logger.info(f"Alpha calculado para {alpha['IDIndicePrincipal'].nunique()} IDs")
            else:
                self.logger.warning("Nenhum dado com CIF_80 > 0 para calcular alpha")
                alpha = pd.DataFrame(columns=['IDIndicePrincipal', 'Data', 'alpha'])
        except Exception as e:
            self.logger.error(f"Erro ao calcular alpha: {str(e)}")
            alpha = pd.DataFrame(columns=['IDIndicePrincipal', 'Data', 'alpha'])
        
        # Pipeline de processamento FOB
        self.logger.info("Processando série FOB_80...")
        series_fob = series[['IDIndicePrincipal', 'Data', 'FOB_80', 'Type']].copy()
        series_fob.rename(columns={'FOB_80': 'Valor'}, inplace=True)
        
        # Preprocessamento
        self.logger.info("Aplicando preprocess_data...")
        try:
            series_preprocessed = series_fob.groupby(['IDIndicePrincipal']).apply(
                preprocess_data, date_column='Data', value_column='Valor'
            ).reset_index(drop=True)
            series_preprocessed.rename(columns={'IDIndicePrincipal': 'ID'}, inplace=True)
            series_preprocessed['ID'] = series_preprocessed['ID'].astype(np.int32)
            series_preprocessed['Type'].fillna('interpolation')
            self.logger.info(f"Séries pré-processadas: {series_preprocessed['ID'].nunique()} IDs")
        except Exception as e:
            self.logger.error(f"Erro ao executar preprocess_data: {str(e)}")
            return pd.DataFrame()
        
        # Outlier testing STL
        self.logger.info("Aplicando outlier_testing_stl...")
        try:
            series_clean = series_preprocessed.groupby(['ID']).apply(
                outlier_testing_stl, date_column='Data', value_column='Valor', id_column='ID'
            ).reset_index(drop=True)
            self.logger.info(f"Séries após STL: {series_clean['ID'].nunique()} IDs")
        except Exception as e:
            self.logger.error(f"Erro ao executar outlier_testing_stl: {str(e)}")
            return pd.DataFrame()
        
        # Filtrar apenas dados target (não históricos)
        series_clean = series_clean[series_clean['Type'] == 'target'].reset_index(drop=True)
        
        # Final interpolation
        self.logger.info("Aplicando final_interpolation...")
        try:
            series_final = series_clean.groupby(['ID']).apply(
                final_interpolation, date_column='Data', value_column='Valor_final'
            ).reset_index(drop=True)
            self.logger.info(f"Séries após interpolação final: {series_final['ID'].nunique()} IDs")
        except Exception as e:
            self.logger.error(f"Erro ao executar final_interpolation: {str(e)}")
            return pd.DataFrame()
        
        # Fix last month high residual
        self.logger.info("Aplicando fix_last_month_high_residual...")
        try:
            series_final3 = series_final.groupby(['ID']).apply(
                fix_last_month_high_residual, date_column='Data', value_column='Valor_final', id_column='ID'
            ).reset_index(drop=True)
            self.logger.info(f"Séries após correção do último mês: {series_final3['ID'].nunique()} IDs")
        except Exception as e:
            self.logger.error(f"Erro ao executar fix_last_month_high_residual: {str(e)}")
            return pd.DataFrame()
        
        # Fix negative values
        self.logger.info("Aplicando fix_negative_values...")
        try:
            series_final4 = fix_negative_values(series_final3)
            self.logger.info(f"Séries após correção de valores negativos: {series_final4['ID'].nunique()} IDs")
        except Exception as e:
            self.logger.error(f"Erro ao executar fix_negative_values: {str(e)}")
            return pd.DataFrame()
        
        # Preparar resultado final FOB
        series_final4.drop(columns=['Valor'], inplace=True)
        series_final4.rename(columns={'Valor_final': 'Valor'}, inplace=True)
        final_fob = series_final4[['ID', 'Data', 'Valor']].copy()
        final_fob['ID'] = final_fob['ID'].astype(np.int32)
        
        # Calcular Valor_Cif
        self.logger.info("Calculando Valor_Cif...")
        if not alpha.empty:
            if 'IDIndicePrincipal' in alpha.columns:
                alpha.rename(columns={'IDIndicePrincipal': 'ID'}, inplace=True)
            final_cif = final_fob.merge(alpha, on=['ID', 'Data'], how='left')
            final_cif['Valor_Cif'] = final_cif['Valor'] * final_cif['alpha']
            final_cif = final_cif[['ID', 'Data', 'Valor', 'Valor_Cif']].copy()
        else:
            self.logger.warning("Alpha vazio, Valor_Cif não será calculado")
            final_cif = final_fob.copy()
            final_cif['Valor_Cif'] = pd.NA
        
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
                        limit_area='inside'
                    )

                # -----------------------------
                # Validação pós-interpolação
                # -----------------------------
                for col in value_cols:
                    first = group_full[col].first_valid_index()
                    last = group_full[col].last_valid_index()

                    if first is not None and last is not None:
                        internal = group_full.loc[first:last, col]
                        if internal.isna().any():
                            raise ValueError(
                                f"Lacunas internas após interpolação "
                                f"(ID={id_}, coluna={col})"
                            )

                dfs.append(group_full)

            result = pd.concat(dfs, ignore_index=True)

            if return_quality_report:
                return result, pd.DataFrame(quality)

            return result

        # gardrail: final interpolation per ID
        final = guardrail_interpolate_time_series(final_cif, value_cols=['Valor', 'Valor_Cif'])

        # Salvar resultado
        self.gold_df = final.copy()
        date_str = datetime.now().strftime('%Y-%m-%d')
        self._save_to_storage(self.gold_df, self.gold_path, f"gold_NM_{date_str}.parquet")
        
        self.logger.info(f"=== FASE 3 CONCLUÍDA ===\n")
        self.logger.info(f"Resultado final: {len(self.gold_df)} registros, {self.gold_df['ID'].nunique()} IDs únicos")
        
        return self.gold_df
    
    # ============= FASE 4: UPLOAD =============
    
    def upload(self, ExcluirHistorico: str = 'N', percentualOutlier: float = 4) -> bool:
        """
        Realiza upload dos dados para a API Cost Drivers.
        
        Primeiro busca dados existentes na API, filtra apenas dados novos,
        e então faz upload usando a lógica de upload_comex.
        
        Args:
            ExcluirHistorico: 'N' ou 'S' para excluir histórico (padrão: 'N')
            percentualOutlier: Percentual de outlier (padrão: 4)
            
        Returns:
            True se upload bem-sucedido, False caso contrário
        """
        self.logger.info("=== INICIANDO FASE 4: UPLOAD ===")

        self.gold_df.to_excel(f"{self.iso_code}_upload.xlsx", index=False)
        raise print("salvou os dados")

        if self.gold_df is None or self.gold_df.empty:
            # date_str = datetime.now().strftime('%Y-%m-%d')
            # self.gold_df = self._load_from_storage(self.gold_path, 
            # f"gold_NM_{date_str}.parquet")
            self.logger.error("gold_df está vazio, não há dados para upload")
            return False
        
        if ApiAsync is None:
            self.logger.error("ApiAsync não disponível, upload não pode ser executado")
            return False
        
        try:
            # Criar funções locais se necessário
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
        except Exception as e:
            self.logger.error(f"Erro ao importar funções necessárias: {str(e)}")
            return False
        
        # Parte 1: Buscar dados existentes da API
        self.logger.info("Buscando dados existentes na API Cost Drivers...")
        final_fob = self.gold_df.copy()
        
        # Preparar datas para busca (últimos 12 meses)
        data_max = final_fob['Data'].max()
        data_min = (data_max - pd.DateOffset(months=12)).strftime("%d-%m-%Y")
        data_max_str = data_max.strftime("%d-%m-%Y")
        
        lista_requisicao = make_lista_requisicao(
            final_fob['ID'].unique().tolist(),
            data_min,
            data_max_str
        )
        
        headers = {'Authorization': 'Bearer ' + get_token_costdrivers()}
        dados_costdrivers = ApiAsync(True, lista_requisicao, headers=headers).run()
        
        # Processar resposta da API
        lista_dataframe = [
            pd.DataFrame(rsp['result'])
            for rsp in dados_costdrivers
            if (not 'Error' in rsp.keys())
        ]
        
        dataframe_costdriver = pd.concat(lista_dataframe, ignore_index=True)
        if not dataframe_costdriver.empty:
            dataframe_costdriver = dataframe_costdriver.query("base == 0")
            dataframe_costdriver = dataframe_costdriver[['indicePrincipalID', 'dataIndice', 'valor']]
            dataframe_costdriver.columns = ['ID', 'Data', 'Valor']
            dataframe_costdriver['Data'] = pd.to_datetime(dataframe_costdriver['Data'], format='%Y-%m-%dT%H:%M:%S')
            
            # Agrupar por ID e pegar data máxima
            df_gb = dataframe_costdriver.groupby(['ID'], as_index=False).max()
            self.logger.info(f"Dados existentes encontrados para {len(df_gb)} IDs")
        else:
            df_gb = pd.DataFrame(columns=['ID', 'Data', 'Valor'])
            self.logger.info("Nenhum dado existente encontrado na API")
        
        # Parte 2: Filtrar dados novos
        self.logger.info("Filtrando apenas dados novos...")
        dfs = []
        
        for id_indice in final_fob['ID'].unique():
            df_fob_id = final_fob[final_fob['ID'] == id_indice].copy()
            df_cost_id = df_gb[df_gb['ID'] == id_indice].copy()
            # 
            if not df_cost_id.empty:
                max_data = df_cost_id['Data'].max()
                df_fob_id = df_fob_id[df_fob_id['Data'] >= max_data]
            # 
            dfs.append(df_fob_id)
        
        df_up = pd.concat(dfs, ignore_index=True)
        self.logger.info(f"Dados novos para upload: {len(df_up)} registros de {df_up['ID'].nunique()} IDs")
        
        if df_up.empty:
            self.logger.info("Nenhum dado novo para upload")
            self.logger.info("=== FASE 4 CONCLUÍDA (sem dados novos) ===\n")
            return True
        
        # Parte 3: Fazer upload usando lógica de upload_comex
        endpoint_cost = 'https://api-costdrivers.gep.com/costdrivers-api'
        
        def create_requisition_list(df_preenchido: pd.DataFrame, ExcluirHistorico: str, percentualOutlier:int) -> List[Dict]:
            """Cria lista de requisições para upload."""
            lista_requisicao = list()
            opc = 9
            idioma = 1
            identity = '2258FB7E-7F19-483E-BBAE-8250973D3658'
            merchantID = ''
            df_preenchido['Data'] = df_preenchido['Data'].dt.strftime("01-%m-%Y")
            for ID, group in df_preenchido.groupby('ID'):
                rjson = {}
                json_df = group.dropna(axis=1, how='all')
                json_df['Base'] = 0
                json_df['Explicativa'] = 0
                json_data = json_df.to_json(orient='records').replace("'", '"')
                jsonSeq = str({'ID': str(ID), 'ExcluirHistorico': ExcluirHistorico, 'Origem': 'Data Science'}).replace("'", '"')
                rjson['opc'] = opc
                rjson['idioma'] = idioma
                rjson['identity'] = identity
                rjson['percentualOutlier'] = percentualOutlier
                rjson['merchantID'] = merchantID
                rjson['json'] = json_data
                rjson['jsonSeq'] = jsonSeq
                lista_requisicao.append(
                    {'url': f'{endpoint_cost}/api/v1/DataScience/UpdateOption-9', 'method': 'put',
                    'json': rjson, 'ID': ID})
            return lista_requisicao
        
        def req_to_df(req):
            """Converte resposta da requisição em DataFrame."""
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
        
        def recursively_upload(lista_requisicao, headers, resp=[]):
            """Faz upload recursivo com retry automático."""
            self.logger.info(f'Fazendo upload de {len(lista_requisicao)} requisições...')
            upload_data = ApiAsync(True, lista_requisicao, headers=headers).run()
            nones_index = [i for i, val in enumerate(upload_data) if val == None]
            if len(nones_index) == len(lista_requisicao):
                self.logger.warning("Não fez mais nenhuma requisição!")
                return resp
            if len(nones_index) > 0:
                lista_requisicao = [val for i, val in enumerate(
                    lista_requisicao) if i in nones_index]
                self.logger.info(f'Reenviando {len(lista_requisicao)} requisições que falharam...')
                if len(resp) > 0:
                    resp += [val for i,
                    val in enumerate(upload_data) if i not in nones_index]
                else:
                    resp = [val for i, val in enumerate(
                        upload_data) if i not in nones_index]
                return recursively_upload(lista_requisicao, headers, resp)
            else:
                if len(resp) == 0:
                    resp = upload_data
                return resp

        # Executar upload
        # lista_requisicao = create_requisition_list(df_up, ExcluirHistorico, percentualOutlier)
        # upload_data = recursively_upload(lista_requisicao, headers)
        # dfs = [req_to_df(req) for req in upload_data]
        # df_uploaded = pd.concat(dfs, ignore_index=True)
        
        # Processar resultados
        success_count = df_uploaded[df_uploaded['success'] == True].shape[0] if 'success' in df_uploaded.columns else 0
        total_count = len(df_uploaded)

        # retornar dados que foram subidos
        self.final_cif = df_up.copy()
        
        self.logger.info(f"Upload concluído: {success_count}/{total_count} registros processados com sucesso")
        
        if success_count == total_count:
            self.logger.info("=== FASE 4 CONCLUÍDA COM SUCESSO ===\n")
            return True
        else:
            self.logger.warning(f"Upload parcial: {success_count}/{total_count} registros processados")
            return False
               
    # ============= MÉTODOS DE STORAGE =============
    
    def _save_to_storage(self, df: pd.DataFrame, path: str, filename: str):
        """
        Salva DataFrame no storage (ADLS2 ou local).
        
        Verifica se arquivo já existe antes de salvar. Se existir e tiver o mesmo tamanho,
        pula o upload para evitar operações desnecessárias.
        
        Args:
            df: DataFrame para salvar
            path: Caminho do diretório (ex: 'staging/comex/ARG/raw')
            filename: Nome do arquivo (ex: 'import_raw.parquet')
        """
        full_path_str = f"{path}/{filename}"
        
        # Verificar se arquivo já existe
        if self._file_exists_in_storage(path, filename):
            try:
                # Carregar arquivo existente para comparar tamanho
                existing_df = self._load_from_storage(path, filename)
                
                # Comparar número de linhas (tamanho)
                if len(existing_df) == len(df):
                    self.logger.info(f"Arquivo {full_path_str} já existe com o mesmo tamanho ({len(df)} registros). Pulando upload.")
                    return
                else:
                    self.logger.info(f"Arquivo {full_path_str} existe mas com tamanho diferente "
                                   f"(existente: {len(existing_df)}, novo: {len(df)}). Substituindo...")
            except Exception as e:
                self.logger.warning(f"Erro ao comparar arquivo existente: {str(e)}. Prosseguindo com upload...")
        
        # Modo desenvolvimento: sempre usar storage local
        if self.developing:
            self.logger.info(f"Modo desenvolvimento: salvando localmente em {path}")
        elif self.use_azure and self.azure_storage:
            try:
                self.logger.info(f"Salvando no ADLS2: {full_path_str}")
                
                azure_path = full_path_str
                if azure_path.startswith('staging/'):
                    azure_path = azure_path.replace('staging/', '', 1)
                
                buffer = BytesIO()
                df.to_parquet(
                    path=buffer,
                    engine='pyarrow',
                    compression='gzip',
                    index=False
                )
                buffer = buffer.getvalue()
                
                self.azure_storage.add_file(buffer=buffer, file_name=azure_path)
                self.logger.info(f"✓ Dados salvos no ADLS2: {azure_path}")
                return
                
            except Exception as e:
                self.logger.error(f"Erro ao salvar no Azure: {str(e)}")
                self.logger.warning("Tentando salvar localmente como fallback...")
        
        # Salvar localmente (modo desenvolvimento ou fallback)
        if self.developing:
            # Modo desenvolvimento: usar path diretamente (já contém NM/dados/ quando em desenvolvimento)
            full_path = Path(path)
        else:
            full_path = Path(path)
        
        full_path.mkdir(parents=True, exist_ok=True)
        
        file_path = full_path / filename
        df.to_parquet(file_path, index=False, compression='gzip')
        self.logger.info(f"✓ Dados salvos localmente: {file_path}")
    
    def _load_from_storage(self, path: str, filename: str) -> pd.DataFrame:
        """
        Carrega DataFrame do storage (ADLS2 ou local).
        
        Args:
            path: Caminho do diretório (ex: 'staging/comex/ARG/raw')
            filename: Nome do arquivo (ex: 'import_raw.parquet' ou 'historical.parquet')
            
        Returns:
            DataFrame carregado
        """
        import re
        
        # Arquivos históricos não devem ter data adicionada (são sempre atualizados)
        is_historical_file = filename == 'historical.parquet'
        
        date_pattern = r'_\d{4}-\d{2}-\d{2}\.parquet$'
        has_date = re.search(date_pattern, filename)
        
        # Se não tem data e não é arquivo histórico, tentar com data de hoje
        if not has_date and not is_historical_file:
            base_name = filename.replace('.parquet', '')
            today_str = datetime.now().strftime('%Y-%m-%d')
            filename_with_date = f"{base_name}_{today_str}.parquet"
            full_path_str = f"{path}/{filename_with_date}"
            
            # Modo desenvolvimento: sempre usar storage local em NM/dados/
            if self.developing:
                # Usar path diretamente (já contém NM/dados/ quando em desenvolvimento)
                full_path = Path(path)
                file_path = full_path / filename_with_date
                if file_path.exists():
                    df = pd.read_parquet(file_path)
                    self.logger.info(f"✓ Dados carregados localmente (dev): {file_path} ({len(df)} registros)")
                    return df
            elif self.use_azure and self.azure_storage:
                azure_path = full_path_str
                if azure_path.startswith('staging/'):
                    azure_path = azure_path.replace('staging/', '', 1)
                
                try:
                    file_bytes = self.azure_storage.download_adls2(azure_path)
                    df = pd.read_parquet(BytesIO(file_bytes))
                    self.logger.info(f"✓ Dados carregados do ADLS2: {azure_path} ({len(df)} registros)")
                    return df
                except Exception as e:
                    self.logger.error(f"Arquivo não encontrado: {azure_path}")
                    self.logger.warning(f"Tentando arquivo sem data: {filename}...")
            else:
                full_path = Path(path)
                file_path = full_path / filename_with_date
                if file_path.exists():
                    df = pd.read_parquet(file_path)
                    self.logger.info(f"✓ Dados carregados localmente: {file_path} ({len(df)} registros)")
                    return df
        
        # Tentar arquivo sem data (ou arquivo histórico que sempre usa nome fixo)
        full_path_str = f"{path}/{filename}"
        
        # Modo desenvolvimento: sempre usar storage local em NM/dados/
        if self.developing:
            # Usar path diretamente (já contém NM/dados/ quando em desenvolvimento)
            full_path = Path(path)
            file_path = full_path / filename
            if file_path.exists():
                df = pd.read_parquet(file_path)
                self.logger.info(f"✓ Dados carregados localmente (dev): {file_path} ({len(df)} registros)")
                return df
            else:
                self.logger.warning(f"Arquivo não encontrado: {file_path}")
                return pd.DataFrame()
        elif self.use_azure and self.azure_storage:
            azure_path = full_path_str
            if azure_path.startswith('staging/'):
                azure_path = azure_path.replace('staging/', '', 1)
            
            try:
                file_bytes = self.azure_storage.download_adls2(azure_path)
                df = pd.read_parquet(BytesIO(file_bytes))
                self.logger.info(f"✓ Dados carregados do ADLS2: {azure_path} ({len(df)} registros)")
                return df
            except Exception as e:
                self.logger.error(f"Erro ao carregar do ADLS2: {str(e)}")
                return pd.DataFrame()
        else:
            full_path = Path(path)
            file_path = full_path / filename
            if file_path.exists():
                df = pd.read_parquet(file_path)
                self.logger.info(f"✓ Dados carregados localmente: {file_path} ({len(df)} registros)")
                return df
            else:
                self.logger.warning(f"Arquivo não encontrado: {file_path}")
                return pd.DataFrame()
    
    def _file_exists_in_storage(self, path: str, filename: str) -> bool:
        """
        Verifica se arquivo existe no storage (ADLS2 ou local).
        
        Args:
            path: Caminho do diretório (ex: 'staging/comex/ARG/raw')
            filename: Nome do arquivo (ex: 'import_raw.parquet' ou 'historical.parquet')
            
        Returns:
            True se arquivo existe, False caso contrário
        """
        full_path_str = f"{path}/{filename}"
        
        # Modo desenvolvimento: sempre verificar localmente em NM/dados/
        if self.developing:
            # Usar path diretamente (já contém NM/dados/ quando em desenvolvimento)
            full_path = Path(path)
            file_path = full_path / filename
            return file_path.exists()
        elif self.use_azure and self.azure_storage:
            azure_path = full_path_str
            if azure_path.startswith('staging/'):
                azure_path = azure_path.replace('staging/', '', 1)
            
            try:
                # Tentar baixar o arquivo para verificar se existe
                # Se não existir, download_adls2 lançará exceção
                self.azure_storage.download_adls2(azure_path)
                return True
            except Exception as e:
                # Se a exceção contém "PathNotFound" ou "não existe", arquivo não existe
                if "PathNotFound" in str(e) or "não existe" in str(e).lower():
                    return False
                # Outras exceções podem indicar problemas de conexão, mas assumimos que não existe
                self.logger.debug(f"Erro ao verificar existência do arquivo {azure_path}: {str(e)}")
                return False
        else:
            # Storage local
            full_path = Path(path)
            file_path = full_path / filename
            return file_path.exists()
    
    # ============= ORQUESTRAÇÃO =============
    
    def run(self, skip_phases: Optional[List[str]] = None) -> pd.DataFrame:
        """
        Executa o pipeline completo: collect -> update_historical -> normalize_historical -> calculate -> upload.
        
        Fluxo:
        1. collect() - Coleta dados brutos
        2. update_historical() - Atualiza/carrega histórico no ADLS
        3. normalize_historical() - Normaliza histórico usando data-contract.yaml antes dos cálculos
        4. calculate() - Executa cálculos usando histórico normalizado
        5. upload() - Faz upload dos resultados
        
        Args:
            skip_phases: Lista de fases para pular (ex: ['upload', 'normalize_historical'])
            
        Returns:
            DataFrame final (gold_df)
        """
        skip_phases = skip_phases or []
        
        self.logger.info("="*60)
        self.logger.info(f"INICIANDO PIPELINE COMEX NM - {self.iso_code}")
        self.logger.info("="*60)
        
        try:
            # Fase 1: Coleta
            if 'collect' not in skip_phases:
                self.collect()
            else:
                self.logger.info("Fase 1 (collect) pulada")
            
            # Fase 2: Atualização do Histórico
            if 'update_historical' not in skip_phases:
                self.update_historical(update_months=3)
            else:
                self.logger.info("Fase 2 (update_historical) pulada")
            
            # Fase 3: Normalização do Histórico
            if 'normalize_historical' not in skip_phases:
                self.normalize_historical()
            else:
                self.logger.info("Fase 3 (normalize_historical) pulada")
            
            # Fase 4: Cálculo
            if 'calculate' not in skip_phases:
                self.calculate()
            else:
                self.logger.info("Fase 4 (calculate) pulada")
            
            # Fase 5: Upload
            if 'upload' not in skip_phases:
                self.upload()
            else:
                self.logger.info("Fase 5 (upload) pulada")
            
            self.logger.info("="*60)
            self.logger.info("PIPELINE CONCLUÍDO COM SUCESSO")
            self.logger.info("="*60)
            
            # Retornar final_cif se disponível, caso contrário retornar gold_df ou DataFrame vazio
            if hasattr(self, 'final_cif') and self.final_cif is not None:
                return self.final_cif
            elif hasattr(self, 'gold_df') and self.gold_df is not None and not self.gold_df.empty:
                return self.gold_df
            else:
                return pd.DataFrame()
            
        except Exception as e:
            self.logger.error(f"Erro durante execução do pipeline: {str(e)}")
            raise

