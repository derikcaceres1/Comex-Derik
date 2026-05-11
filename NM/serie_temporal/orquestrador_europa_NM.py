"""
Orquestrador para executar pipelines COMEX de todos os países europeus usando Nova Metodologia.

Este script executa sequencialmente os pipelines de:
- Áustria (AUT)
- Itália (ITA)
- França (FRA)
- Bélgica (BEL)
- Alemanha (DEU)
- Portugal (PRT)
- Hungria (HUN)
- Bulgária (BGR)
- Holanda (NLD)

Cada pipeline processa dados do país específico a partir dos dados coletados do EUR no ADLS.
"""

from datetime import datetime
import traceback
from typing import Dict, List, Tuple
import pandas as pd
import sys
from pathlib import Path

# Adicionar path do diretório serie_temporal ao sys.path para imports
serie_temporal_path = Path(__file__).parent
if str(serie_temporal_path) not in sys.path:
    sys.path.insert(0, str(serie_temporal_path))

from COMEX_AUT.COMEX_AUT_NM import COMEX_AUT_NM
from COMEX_ITA.COMEX_ITA_NM import COMEX_ITA_NM
from COMEX_FRA.COMEX_FRA_NM import COMEX_FRA_NM
from COMEX_BEL.COMEX_BEL_NM import COMEX_BEL_NM
from COMEX_DEU.COMEX_DEU_NM import COMEX_DEU_NM
from COMEX_PRT.COMEX_PRT_NM import COMEX_PRT_NM
from COMEX_HUN.COMEX_HUN_NM import COMEX_HUN_NM
from COMEX_BGR.COMEX_BGR_NM import COMEX_BGR_NM
from COMEX_NLD.COMEX_NLD_NM import COMEX_NLD_NM


# Configuração dos países
PAISES_CONFIG = [
    {
        'nome': 'Áustria',
        'iso_code': 'AUT',
        'classe': COMEX_AUT_NM
    },
    {
        'nome': 'Itália',
        'iso_code': 'ITA',
        'classe': COMEX_ITA_NM
    },
    {
        'nome': 'França',
        'iso_code': 'FRA',
        'classe': COMEX_FRA_NM
    },
    {
        'nome': 'Bélgica',
        'iso_code': 'BEL',
        'classe': COMEX_BEL_NM
    },
    {
        'nome': 'Alemanha',
        'iso_code': 'DEU',
        'classe': COMEX_DEU_NM
    },
    {
        'nome': 'Portugal',
        'iso_code': 'PRT',
        'classe': COMEX_PRT_NM
    },
    {
        'nome': 'Hungria',
        'iso_code': 'HUN',
        'classe': COMEX_HUN_NM
    },
    {
        'nome': 'Bulgária',
        'iso_code': 'BGR',
        'classe': COMEX_BGR_NM
    },
    {
        'nome': 'Holanda',
        'iso_code': 'NLD',
        'classe': COMEX_NLD_NM
    }
]


def executar_pais(config: Dict, start_date=None, use_azure=True, developing=False) -> Tuple[bool, str, pd.DataFrame]:
    """
    Executa o pipeline de um país específico.
    
    Args:
        config: Dicionário com configuração do país (nome, iso_code, classe)
        start_date: Data de início para filtro (opcional)
        use_azure: Se deve usar Azure Storage (padrão: True)
        developing: Se está em modo desenvolvimento (padrão: False)
        
    Returns:
        Tupla (sucesso, mensagem, resultado_df)
    """
    nome = config['nome']
    iso_code = config['iso_code']
    classe = config['classe']
    
    try:
        print(f"\n{'='*80}")
        print(f"Iniciando pipeline para {nome} ({iso_code})")
        print(f"{'='*80}")
        
        # Criar instância do pipeline
        pipeline = classe(
            start_date=start_date,
            use_azure=use_azure,
            developing=developing
        )
        
        # Executar pipeline completo
        result_df = pipeline.run()
        
        # Verificar resultado
        if result_df is not None and not result_df.empty:
            mensagem = f"Pipeline {nome} concluído com sucesso. Total de registros: {len(result_df)}"
            print(f"✓ {mensagem}")
            return True, mensagem, result_df
        else:
            mensagem = f"Pipeline {nome} concluído mas sem dados retornados"
            print(f"⚠ {mensagem}")
            return True, mensagem, pd.DataFrame()
            
    except Exception as e:
        mensagem = f"Erro ao executar pipeline {nome}: {str(e)}"
        print(f"✗ {mensagem}")
        print(f"Traceback:\n{traceback.format_exc()}")
        return False, mensagem, pd.DataFrame()


def executar_todos_paises(
    start_date=None,
    use_azure=True,
    developing=False,
    paises_especificos: List[str] = None
) -> Dict[str, Tuple[bool, str, pd.DataFrame]]:
    """
    Executa pipelines de todos os países europeus configurados.
    
    Args:
        start_date: Data de início para filtro (opcional)
        use_azure: Se deve usar Azure Storage (padrão: True)
        developing: Se está em modo desenvolvimento (padrão: False)
        paises_especificos: Lista de códigos ISO para executar apenas países específicos (opcional)
        
    Returns:
        Dicionário com resultados de cada país: {iso_code: (sucesso, mensagem, resultado_df)}
    """
    resultados = {}
    inicio_geral = datetime.now()
    
    print(f"\n{'#'*80}")
    print(f"# ORQUESTRADOR COMEX EUROPA - NOVA METODOLOGIA")
    print(f"# Início: {inicio_geral.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'#'*80}")
    
    # Filtrar países se especificado
    paises_para_executar = PAISES_CONFIG
    if paises_especificos:
        paises_para_executar = [
            p for p in PAISES_CONFIG 
            if p['iso_code'] in paises_especificos
        ]
        print(f"\nExecutando apenas países específicos: {', '.join(paises_especificos)}")
    
    # Executar cada país
    sucessos = 0
    falhas = 0
    
    for i, config in enumerate(paises_para_executar, 1):
        print(f"\n[{i}/{len(paises_para_executar)}] Processando {config['nome']}...")
        
        sucesso, mensagem, resultado = executar_pais(
            config,
            start_date=start_date,
            use_azure=use_azure,
            developing=developing
        )
        
        resultados[config['iso_code']] = (sucesso, mensagem, resultado)
        
        if sucesso:
            sucessos += 1
        else:
            falhas += 1
    
    # Resumo final
    fim_geral = datetime.now()
    duracao = fim_geral - inicio_geral
    
    print(f"\n{'#'*80}")
    print(f"# RESUMO DA EXECUÇÃO")
    print(f"{'#'*80}")
    print(f"Total de países processados: {len(paises_para_executar)}")
    print(f"Sucessos: {sucessos}")
    print(f"Falhas: {falhas}")
    print(f"Duração total: {duracao}")
    print(f"Fim: {fim_geral.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'#'*80}\n")
    
    # Detalhes por país
    print("Detalhes por país:")
    for iso_code, (sucesso, mensagem, resultado) in resultados.items():
        status = "✓" if sucesso else "✗"
        registros = len(resultado) if not resultado.empty else 0
        print(f"  {status} {iso_code}: {mensagem} ({registros} registros)")
    
    return resultados


def main():
    """Função principal para execução local."""
    # Configurações
    start_date = None  # None para processar todos os dados disponíveis
    use_azure = False  # False para desenvolvimento local
    developing = True   # True para modo desenvolvimento
    
    # Executar todos os países
    resultados = executar_todos_paises(
        start_date=start_date,
        use_azure=use_azure,
        developing=developing
    )
    
    # Salvar resumo em arquivo
    timestamp = datetime.now().strftime('%Y%m%dT%H%M')
    resumo_path = f'resumo_orquestrador_europa_NM_{timestamp}.txt'
    
    with open(resumo_path, 'w', encoding='utf-8') as f:
        f.write("RESUMO DA EXECUÇÃO DO ORQUESTRADOR COMEX EUROPA\n")
        f.write("="*80 + "\n\n")
        
        for iso_code, (sucesso, mensagem, resultado) in resultados.items():
            status = "SUCESSO" if sucesso else "FALHA"
            registros = len(resultado) if not resultado.empty else 0
            f.write(f"{iso_code}: {status}\n")
            f.write(f"  {mensagem}\n")
            f.write(f"  Registros: {registros}\n\n")
    
    print(f"\nResumo salvo em: {resumo_path}")
    
    return resultados


if __name__ == "__main__":
    resultados = main()
    
    # Verificar se houve falhas
    falhas = sum(1 for _, (sucesso, _, _) in resultados.items() if not sucesso)
    if falhas > 0:
        print(f"\n⚠ Atenção: {falhas} pipeline(s) falharam!")
        exit(1)
    else:
        print("\n✓ Todos os pipelines foram executados com sucesso!")
        exit(0)

