# 📘 Projeto COMEX – Documentação Geral

## 1. Visão Geral

Este projeto é responsável pelo **processamento, cálculo, projeção e atualização de indicadores de Comércio Exterior (COMEX)**. Ele foi estruturado para suportar **duas metodologias distintas**:

* **OM (Old Methodology)** – metodologia antiga, mantida por compatibilidade histórica
* **NM (New Methodology)** – nova metodologia, mais moderna, modular e escalável

O projeto foi organizado para:

* Permitir evolução da metodologia sem quebrar processos antigos
* Facilitar manutenção e entendimento por novos integrantes
* Separar regras comuns de regras específicas por país

---

## 2. Estrutura Geral do Projeto

```text
📦 raiz_do_projeto
 ├── docs/              # Documentação HTML completa e navegável
 ├── library/           # Código e arquivos compartilhados
 ├── NM/                # Nova metodologia
 ├── OM/                # Metodologia antiga (legado)
 ├── data-contract.yaml # Contrato de dados (mapeamento de colunas)
 ├── requirements.txt   # Dependências do projeto
 └── COMO_USAR_O_PROJETO.md  # Guia de uso prático
```

Cada pasta possui uma responsabilidade clara, explicada a seguir.

---

## 2.1 Documentação HTML (`docs/`)

### 📂 O que é

A pasta `docs/` contém **documentação HTML completa e navegável** de todos os módulos do projeto.

### 📌 Arquivos disponíveis

* `index.html` - Página principal com índice de todas as documentações
* `documentacao_OM.html` - Documentação completa da Metodologia Antiga
* `documentacao_globinho.html` - Documentação do módulo NM/globinho
* `documentacao_NM.html` - Documentação do módulo NM/serie_temporal
* `documentacao_projecao_cif.html` - Documentação do processo de projeção CIF

### 🌐 Como usar

1. Abra `docs/index.html` no navegador
2. Navegue entre as documentações usando os links no topo ou rodapé de cada página
3. Cada documentação contém:
   * Visão geral do módulo
   * Estrutura de arquivos
   * Fluxos de processamento detalhados
   * Exemplos de uso
   * Troubleshooting

---

---

## 3. Pasta `library/` – Código Compartilhado

### 📂 O que é

A pasta `library` funciona como uma **biblioteca central** do projeto, utiliza dos mesmos arquivo usados em todos projetos de RPA.

Ela contém códigos e arquivos que são utilizados por **múltiplos processos**, tanto da metodologia nova quanto da antiga.

### 📌 Exemplos do que pode existir aqui

* Funções utilitárias (datas, validações, conversões)
* Funções de leitura e escrita de arquivos (Parquet, CSV, etc.)
* Configurações comuns
* Constantes e padrões globais

## 4. Pasta `NM/` – Nova Metodologia

A pasta `NM` concentra **toda a lógica da nova metodologia de cálculo**.

```text
NM/
 ├── dados/              # Dados processados por país (Parquet)
 ├── globinho/           # Cálculo do indicador Globinho
 ├── projecao_cif/       # Projeção de valores CIF
 └── serie_temporal/     # Atualização de séries temporais
```

Cada subpasta representa um **processo específico**, explicado abaixo.

---

## 4.1 `NM/globinho/` – Cálculo do Indicador Globinho

### 📌 Objetivo

Responsável por **calcular e atualizar os dadps do "Globinho"**.

Este processo combina:

* Regras gerais (comuns a todos os países)
* Regras específicas por país

### 📂 Estrutura completa

```text
NM/globinho/
 ├── comex_globe_pipeline.py    # Pipeline base abstrato
 ├── config_comex.json          # Configurações gerais
 ├── tblNCM_Pais.xlsx           # Tabela de referência NCM/Pais
 ├── sql/
 │   ├── funcao_airflow.py      # Função Airflow para carga alternativa
 │   └── script_processamento_batch.sql  # Script SQL para processamento batch
 ├── ARG/
 │   ├── COMEX_ARG_GLOBE.py
 │   └── paisID_ARG.csv
 ├── BRA/
 │   ├── COMEX_BRA_GLOBE.py
 │   └── paisID_BRA.csv
 ├── COL/
 │   ├── COMEX_COL_GLOBE.py
 │   └── codpais_m49_colombia.xlsx
 ├── EUR/
 │   ├── COMEX_EUR_GLOBE.py
 │   └── PARTNERS_ISO.txt
 └── USA/
     └── COMEX_USA_GLOBE.py
```

### 🔄 Fluxo Alternativo (Airflow + SQL)

Para grandes volumes de dados, existe um fluxo alternativo que contorna problemas de performance da API:

1. Dados são processados e preparados localmente
2. Arquivo Parquet é enviado manualmente para o fileshare do Airflow (na pasta `include`)
3. `funcao_airflow.py` é executado no Airflow para carregar dados no SQL Server (já está no arquivo [costdriver_db_ETL](https://airflowcostdrivers.gep.com/dags/costdriver_db_ETL/grid?tab=code))
4. `script_processamento_batch.sql` é executado no banco de produção para processar os dados

### 🌍 Países suportados no Globinho

* **ARG** (Argentina)
* **BRA** (Brasil)
* **COL** (Colômbia)
* **EUR** (Europa - agregado)
* **USA** (Estados Unidos)

### 🧠 Como funciona

1. O código comum define a lógica padrão
2. Cada país possui sua própria pasta
3. Dentro da pasta do país estão:

   * O código de atualização específico
   * Os arquivos de dados necessários

Isso permite que cada país tenha exceções sem quebrar o fluxo geral.

---

## 4.2 `NM/projecao_cif/` – Projeção de Valores CIF

### 📌 Objetivo

Responsável por **calcular valores CIF faltantes** com base na projeção do FOB, utilizando razões históricas (alpha) entre CIF e FOB (já está no arquivo [costdriver_db_ETL](https://airflowcostdrivers.gep.com/dags/costdriver_db_ETL/grid?tab=code)).

### 📂 Estrutura

```text
NM/projecao_cif/
 └── processamento_BR.sql
```

### 🧠 Observações importantes

* Processo SQL puro executado na DAG `costdriver_db_ETL` do Airflow
* Calcula valores CIF faltantes multiplicando a projeção do FOB pela razão histórica média mensal
* **DEVE ser executado após qualquer atualização ou modificação nos dados do Brasil (más também é executado diariamente por segurança)**
* Conecta diretamente ao banco de dados de produção
* Processa apenas dados de importação do Brasil (paisID = 27)

---

## 4.3 `NM/serie_temporal/` – Atualização de Séries Temporais

### 📌 Objetivo

Atualizar as **séries temporais da nova metodologia**, mantendo histórico e consistência dos dados.

### 📂 Estrutura completa

```text
NM/serie_temporal/
 ├── costdrivers_comex_NM.py        # Classe base para pipelines NM
 ├── data-contract.yaml             # Contrato de dados (mapeamento de colunas)
 ├── orquestrador_europa_NM.py      # Orquestrador para países europeus
 ├── COMEX_AUT/                     # Áustria
 │   └── COMEX_AUT_NM.py
 ├── COMEX_BEL/                     # Bélgica
 │   └── COMEX_BEL_NM.py
 ├── COMEX_BGR/                     # Bulgária
 │   └── COMEX_BGR_NM.py
 ├── COMEX_BRA/                     # Brasil
 │   └── COMEX_BRA_NM.py
 ├── COMEX_DEU/                     # Alemanha
 │   └── COMEX_DEU_NM.py
 ├── COMEX_FRA/                     # França
 │   └── COMEX_FRA_NM.py
 ├── COMEX_HUN/                     # Hungria
 │   └── COMEX_HUN_NM.py
 ├── COMEX_ITA/                     # Itália
 │   └── COMEX_ITA_NM.py
 ├── COMEX_NLD/                     # Holanda
 │   └── COMEX_NLD_NM.py
 └── COMEX_PRT/                     # Portugal
     └── COMEX_PRT_NM.py
```

### 📋 Contrato de Dados (`data-contract.yaml`)

O arquivo `data-contract.yaml` especifica como as colunas brutas de cada país são mapeadas para colunas normalizadas. 

**⚠️ Importante:** 
* Pode haver diferenças entre colunas de importação e exportação
* Cada país pode ter nomenclaturas diferentes (ex: `PRODUCT_NC` na Europa vs `CO_NCM` no Brasil)
* Deve ser feito com cuidado ao adicionar novos países

### 🌍 Países suportados nas Séries Temporais

* **AUT** (Áustria)
* **BEL** (Bélgica)
* **BGR** (Bulgária)
* **BRA** (Brasil)
* **DEU** (Alemanha)
* **FRA** (França)
* **HUN** (Hungria)
* **ITA** (Itália)
* **NLD** (Holanda)
* **PRT** (Portugal)

### 🎯 Orquestrador Europeu

O arquivo `orquestrador_europa_NM.py` executa sequencialmente os pipelines de todos os países europeus (AUT, ITA, FRA, BEL, DEU, PRT, HUN, BGR, NLD).

**⚠️ Importante sobre dados europeus:**
* Cada país europeu usa seu próprio arquivo histórico pré-separado: `NM/dados/{ISO_CODE}/database/historical.parquet`
* Estes arquivos são atualizados pelo processo `OM/COMEX_EUR/COMEX_EUR.py` para evitar processamento redundante
* O processo EUR centraliza a coleta e separação dos dados por país

### 🧠 Como funciona

* Código comum trata regras gerais
* Cada país possui:

  * Seu próprio código de atualização
  * Sua base de dados em formato Parquet

Essa abordagem facilita manutenção e evolução incremental.

---

## 4.4 `NM/dados/` – Armazenamento de Dados Processados

### 📌 Objetivo

Armazena os **dados processados** pela nova metodologia, organizados por país em formato Parquet.

### 📂 Estrutura de dados

```text
NM/dados/
 ├── ARG/
 │   ├── export_hist_arg_raw.parquet
 │   └── import_hist_arg_raw.parquet
 ├── AUT/
 │   ├── database/
 │   │   └── historical.parquet
 │   └── gold/
 │       └── gold_NM_YYYY-MM-DD.parquet
 ├── BEL/
 │   ├── database/
 │   │   └── historical.parquet
 │   └── gold/
 │       └── gold_NM_YYYY-MM-DD.parquet
 ├── BGR/
 ├── BRA/
 ├── DEU/
 ├── FRA/
 ├── HUN/
 ├── ITA/
 ├── NLD/
 └── PRT/
```

### 📊 Camadas de dados

* **raw/**: Dados brutos históricos (apenas ARG)
* **database/**: Dados históricos processados (países europeus)
* **gold/**: Dados finais processados com timestamps (camada gold)

---

## 5. Pasta `OM/` – Metodologia Antiga (Legado)

### 📌 Objetivo

A pasta `OM` contém a **metodologia antiga de cálculo**, mantida por motivos de:

* Histórico
* Auditoria
* Comparação com a nova metodologia

### 📂 Estrutura completa

```text
OM/
 ├── comex_framework.html          # Framework HTML comum
 ├── costdrivers_comex_OM.py       # Funções compartilhadas de cost drivers
 ├── dados/                        # Dados processados (camadas raw/silver/gold)
 │   ├── ARG/
 │   │   ├── raw/
 │   │   ├── silver/
 │   │   └── gold/
 ├── COMEX_ARG/                    # Argentina
 │   ├── COMEX_ARG.py
 │   ├── COMEX_ARG_DAG.py          # DAG para Airflow
 │   └── comex_ARG.html
 ├── COMEX_BRA/                    # Brasil
 │   ├── COMEX_BRA.py
 │   ├── COMEX_BRA_DAG.py
 │   └── comex_BRA.html
 ├── COMEX_COL/                    # Colômbia
 │   ├── COMEX_COL.py
 │   ├── COMEX_COL_DAG.py
 │   └── comex_COL.html
 ├── COMEX_DEU/                    # Alemanha
 │   ├── COMEX_DEU.py
 │   ├── COMEX_DEU_DAG.py
 │   └── comex_DEU.html
 ├── COMEX_EUR/                    # Europa (agregado)
 │   ├── COMEX_EUR.py
 │   ├── COMEX_EUR_DAG.py
 │   └── comex_EUR.html
 ├── COMEX_IND/                    # Índia
 │   ├── COMEX_IND.py
 │   └── comex_IND.html
 ├── COMEX_MEX/                    # México
 │   ├── COMEX_MEX.py
 │   ├── valores_export.html
 │   ├── valores_import.html
 │   ├── volumes_export.html
 │   └── volumes_import.html
 └── COMEX_USA/                    # Estados Unidos
     ├── COMEX_USA.py
     ├── COMEX_USA_DAG.py
     └── comex_USA.html
```

### 🌍 Países suportados na Metodologia Antiga

* **ARG** (Argentina) - com DAG
* **BRA** (Brasil) - com DAG
* **COL** (Colômbia) - com DAG
* **DEU** (Alemanha) - com DAG
* **EUR** (Europa) - com DAG
* **IND** (Índia) - sem DAG
* **MEX** (México) - sem DAG
* **USA** (Estados Unidos) - com DAG

### 📊 Estrutura de dados OM

A pasta `OM/dados/` **simula a saída dos processos COMEX da metodologia antiga**. Quando `use_azure=False`, os dados são salvos localmente nesta estrutura seguindo a arquitetura de camadas:

* **raw/**: Dados brutos extraídos das fontes externas
* **silver/**: Dados limpos, normalizados e transformados
* **gold/**: Dados finais processados com cálculos aplicados e timestamps

**Observação:** Esta estrutura permite desenvolvimento e testes locais antes de fazer deploy para Azure ADLS2.

---

## 6. Guia Rápido 

| Quero entender…              | Onde olhar                    |
| ---------------------------- | ----------------------------- |
| **Documentação completa**    | `docs/index.html` (abrir no navegador) |
| Visão geral do projeto       | README.md (este arquivo)      |
| Como usar o projeto          | `COMO_USAR_O_PROJETO.md`      |
| Dependências do projeto      | `requirements.txt`            |
| Código reutilizável          | `library/`                    |
| Contrato de dados            | `NM/serie_temporal/data-contract.yaml` |
| Nova metodologia             | `NM/`                         |
| Indicador Globinho           | `NM/globinho/`                |
| Projeção CIF                 | `NM/projecao_cif/`            |
| Séries temporais             | `NM/serie_temporal/`          |
| Dados processados (NM)       | `NM/dados/`                   |
| Orquestrador países europeus | `NM/serie_temporal/orquestrador_europa_NM.py` |
| Metodologia antiga           | `OM/`                         |
| Dados processados (OM)       | `OM/dados/` (simula saída dos processos) |
| DAGs Airflow (OM)            | `OM/COMEX_*/COMEX_*_DAG.py`   |
| DAG Projeção CIF             | `costdriver_db_ETL` (Airflow) |

---

## 7. Estrutura de Dados

### 7.1 Metodologia Nova (NM)

Os dados são armazenados em formato **Parquet** na pasta `NM/dados/`:

* **Países europeus**: Estrutura com `database/` (histórico) e `gold/` (dados finais timestampados)
* **Argentina**: Dados raw históricos separados por tipo (export/import)
* **Brasil**: Apenas camada `database/`

### 7.2 Metodologia Antiga (OM)

Os dados seguem arquitetura de **camadas** (raw → silver → gold) na pasta `OM/dados/`:

* **raw/**: Dados brutos extraídos das fontes (formato: `{tipo}_raw_{YYYY-MM-DD}.parquet`)
* **silver/**: Dados limpos, normalizados e transformados (formato: `silver_{YYYY-MM-DD}.parquet`)
* **gold/**: Dados finais processados com cálculos aplicados (formato: `gold_{YYYY-MM-DD}.parquet`)

**⚠️ Importante:** A pasta `OM/dados/` simula a saída dos processos COMEX da metodologia antiga. Em produção, os dados são salvos no Azure ADLS2 em `staging/comex/{ISO_CODE}/{camada}/`.

---

## 8. Arquivos de Configuração na Raiz

### 8.1 `data-contract.yaml`

Contrato de dados que especifica o mapeamento de colunas brutas para colunas normalizadas por país. 

**Localização:** `NM/serie_temporal/data-contract.yaml`

**Uso:** Utilizado tanto pela metodologia antiga (OM) quanto pela nova (NM) para normalização de dados.

### 8.2 `requirements.txt`

Lista todas as dependências Python do projeto com versões específicas.

**Instalação:**
```bash
pip install -r requirements.txt
```

### 8.3 `COMO_USAR_O_PROJETO.md`

Guia prático de uso do projeto com exemplos de execução de cada módulo.

---