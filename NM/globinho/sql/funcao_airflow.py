@task
def task_load_comex_parquet_to_sql():
    import pyodbc
    import pandas as pd

    # --- cria connection string sqlalchemy/pyodbc ---
    conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={server};DATABASE={database};UID={username};PWD={password};"
        "Encrypt=yes;TrustServerCertificate=yes;"
    )

    # --- caminho do arquivo Parquet ---
    parquet_path = './include/comex_europa_final.parquet'

    # --- lê o arquivo Parquet ---
    df = pd.read_parquet(parquet_path)
    
    # ids = [6148, 246509]
    # df = df[df['idIndicador'].isin(ids)]
    print(f"Lido {len(df)} registros do arquivo Parquet.")

    # --- padroniza os nomes das colunas para evitar duplicatas por case ---
    df.columns = [col.strip().lower().capitalize() for col in df.columns]

    # --- define os nomes esperados e garante que todas existam ---
    expected_columns = [
        'Idindicador', 'Datancm', 'Unid', 'Importexport', 'Idpais', 'Uf', 'Via',
        'Quantidade', 'Peso', 'Fob', 'Frete', 'Seguro', 'Valorcif', 'Processado'
    ]

    for col in expected_columns:
        if col not in df.columns:
            df[col] = None  # adiciona coluna ausente com valores nulos

    # --- reordena colunas para garantir a ordem correta ---
    df = df[expected_columns]

    # --- cria a tabela no SQL Server (se ainda não existir) ---
    create_table_sql = """
    IF OBJECT_ID('dbo.TempInternationalTrade', 'U') IS NULL
    CREATE TABLE dbo.TempInternationalTrade (
        IDIndicador INT,
        DataNCM DATE,
        UNID VARCHAR(MAX),
        ImportExport INT,
        IDPais INT,
        UF VARCHAR(MAX),
        VIA VARCHAR(MAX),
        Quantidade DECIMAL(20,6),
        Peso DECIMAL(20,6),
        FOB DECIMAL(20,6),
        Frete DECIMAL(20,6),
        Seguro DECIMAL(20,6),
        ValorCIF DECIMAL(20,6),
        processado BIT DEFAULT 0
    );
    """

    # --- conecta via pyodbc ---
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    cursor.fast_executemany = True

    # --- cria a tabela se necessário ---
    cursor.execute(create_table_sql)
    conn.commit()
    print("Tabela TempInternationalTrade verificada/criada com sucesso.")

    # --- prepara os dados para inserção ---
    records = df.where(pd.notnull(df), None).values.tolist()

    # --- define o insert SQL ---
    insert_sql = """
    INSERT INTO dbo.TempInternationalTrade (
        IDIndicador, DataNCM, UNID, ImportExport, IDPais, UF, VIA,
        Quantidade, Peso, FOB, Frete, Seguro, ValorCIF, processado
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    # --- insere em batches de 100.000 ---
    batch_size = 100_000
    total = len(records)
    print(f"Iniciando inserção de {total} registros em batches de {batch_size}...")

    for i in range(0, total, batch_size):
        batch = records[i:i + batch_size]
        cursor.executemany(insert_sql, batch)
        conn.commit()
        print(f"Batch {i // batch_size + 1} inserido: {len(batch)} registros.")

    # --- fecha conexão ---
    cursor.close()
    conn.close()
    
    # remove o arquivo parquet após a carga
    import os
    os.remove(parquet_path)