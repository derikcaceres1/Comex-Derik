# 📖 Como Usar o Projeto COMEX

Este guia fornece instruções práticas para executar os diferentes componentes do projeto COMEX.

---

## 🔧 Pré-requisitos

### Instalação de Dependências

1. Certifique-se de ter Python 3.8+ instalado
2. Instale as dependências do projeto:

```bash
pip install -r requirements.txt
```

### Variáveis de Ambiente

**Variáveis obrigatórias:**
- `COSTDRIVERS_PASSWORD` - Senha para autenticação CostDrivers
- `COSTDRIVERS_API_KEY` - Chave da API CostDrivers
- `COSTDRIVERS_API_EMAIL` - Email para autenticação
- `COSTDRIVERS_API_PASSWORD` - Senha da API

**Variáveis opcionais:**
- `COSTDRIVERS_ENDPOINT` - Endpoint da API (padrão: `https://api-costdrivers.gep.com/costdrivers-api`)
- `DATALAKE_SAS_TOKEN` - Token SAS para Azure Storage
- `AIRFLOW_INCLUDE_PATH` - Path do diretório include do Airflow (padrão: `/opt/airflow/include`)

---

## ⚙️ Configuração Inicial

### Opção 1: Arquivo .env (Recomendado para Desenvolvimento)

Crie um arquivo `.env` na raiz do projeto:

```bash
COSTDRIVERS_PASSWORD=sua_senha
COSTDRIVERS_API_KEY=sua_chave
COSTDRIVERS_API_EMAIL=seu_email@exemplo.com
COSTDRIVERS_API_PASSWORD=sua_senha_api
COSTDRIVERS_ENDPOINT=https://api-costdrivers.gep.com/costdrivers-api
DATALAKE_SAS_TOKEN=seu_token_sas
```
---

## 🆕 Executando Scripts da Nova Metodologia (NM)

### 📍 Executando da Raiz do Projeto

**Importante:** Sempre execute os scripts a partir da raiz do projeto (`comex/`) para garantir que os imports funcionem corretamente.

### Séries Temporais por País

Execute scripts individuais de séries temporais usando o módulo Python:

```bash
# Formato geral:
python -m NM.serie_temporal.COMEX_<PAIS>.COMEX_<PAIS>_NM

# Exemplos:
python -m NM.serie_temporal.COMEX_BRA.COMEX_BRA_NM
python -m NM.serie_temporal.COMEX_BGR.COMEX_BGR_NM
python -m NM.serie_temporal.COMEX_AUT.COMEX_AUT_NM
...
```

### Executando Diretamente pelo Arquivo

Alternativamente, você pode executar diretamente pelo arquivo (certifique-se de estar no diretório correto):

```bash
# Navegue até o diretório do país
cd NM/serie_temporal/COMEX_BRA

# Execute o script
python COMEX_BRA_NM.py
```

---

## 🔄 Executando Scripts da Metodologia Antiga (OM)

### Séries Temporais por País

```bash
# Formato geral:
python -m OM.COMEX_<PAIS>.COMEX_<PAIS>

# Exemplos:
python -m OM.COMEX_BRA.COMEX_BRA
python -m OM.COMEX_ARG.COMEX_ARG
python -m OM.COMEX_COL.COMEX_COL
...
```

### Executando Diretamente pelo Arquivo

```bash
# Navegue até o diretório do país
cd OM/COMEX_BRA

# Execute o script
python COMEX_BRA.py
```

---

## 🌍 Executando o Globinho

O Globinho é um indicador calculado pela Nova Metodologia. Execute scripts específicos por país:

### Executando da Raiz do Projeto

```bash
# Formato geral:
python -m NM.globinho.<PAIS>.COMEX_<PAIS>_GLOBE

# Exemplos:
python -m NM.globinho.ARG.COMEX_ARG_GLOBE
python -m NM.globinho.BRA.COMEX_BRA_GLOBE
python -m NM.globinho.COL.COMEX_COL_GLOBE
...
```

### Executando Diretamente pelo Arquivo

```bash
# Navegue até o diretório do país
cd NM/globinho/ARG

# Execute o script
python COMEX_ARG_GLOBE.py
```

### Usando o Pipeline Base

O pipeline base (`comex_globe_pipeline.py`) contém a classe abstrata `BaseComexProcessor` que é herdada por cada país. Para usar programaticamente:

```python
from NM.globinho.ARG.COMEX_ARG_GLOBE import COMEX_ARG_GLOBE

processor = COMEX_ARG_GLOBE()
processor.run()
```

---

## 🎯 Executando o Orquestrador Europeu

O orquestrador executa sequencialmente todos os países europeus da Nova Metodologia.

### Executando da Raiz do Projeto

```bash
# Navegue até o diretório serie_temporal
cd NM/serie_temporal

# Execute o orquestrador
python orquestrador_europa_NM.py
```

### Executando como Módulo

```bash
# Da raiz do projeto
python -m NM.serie_temporal.orquestrador_europa_NM
```

### Países Processados pelo Orquestrador

O orquestrador processa os seguintes países na ordem:
1. Áustria (AUT)
2. Itália (ITA)
3. França (FRA)
4. Bélgica (BEL)
5. Alemanha (DEU)
6. Portugal (PRT)
7. Hungria (HUN)
8. Bulgária (BGR)
9. Holanda (NLD)

---

## 🔍 Troubleshooting

### Erro: "ModuleNotFoundError" ou "No module named"

**Causa:** Executando o script do diretório errado ou Python não encontra os módulos.

**Solução:**
1. Certifique-se de estar na raiz do projeto (`comex/`)
2. Use o formato `python -m` para executar como módulo
3. Verifique se o diretório raiz está no PYTHONPATH

```bash
# Exemplo correto:
cd c:\Users\danilo.giomo\documents\comex
python -m NM.serie_temporal.COMEX_BRA.COMEX_BRA_NM
```

### Erro: "Variável de ambiente não encontrada"

**Causa:** Variáveis de ambiente não configuradas.

**Solução:**
1. Verifique se o arquivo `.env` existe na raiz do projeto
2. Verifique se as variáveis estão configuradas no sistema
3. Consulte [`docs/VARIAVEIS_AMBIENTE.md`](docs/VARIAVEIS_AMBIENTE.md) para lista completa

### Erro: "Arquivo de configuração não encontrado"

**Causa:** Arquivo `config_comex.json` não encontrado (para scripts do Globinho).

**Solução:**
1. Verifique se `NM/globinho/config_comex.json` existe
2. Execute sempre da raiz do projeto
3. O código tenta encontrar automaticamente em múltiplos locais

### Erro: "Import não funciona"

**Causa:** Problemas com imports relativos ou PYTHONPATH.

**Solução:**
1. Execute sempre da raiz do projeto usando `python -m`
2. Verifique se `library/` está acessível
3. Os scripts já configuram `sys.path` automaticamente

### Erro ao Executar no Airflow

**Causa:** Paths ou variáveis de ambiente diferentes no ambiente Airflow.

**Solução:**
1. Configure `AIRFLOW_INCLUDE_PATH` se necessário
2. Verifique se as variáveis estão configuradas no Airflow Variables
3. Os DAGs já configuram `sys.path` automaticamente

---

## 💡 Dicas

1. **Sempre execute da raiz:** Use `python -m` a partir da raiz do projeto para garantir imports corretos
2. **Verifique variáveis de ambiente:** Muitos erros são causados por variáveis não configuradas
3. **Use logs:** Os scripts geram logs detalhados para debug
4. **Teste localmente primeiro:** Execute scripts localmente antes de usar em produção
5. **Consulte a documentação:** Cada módulo tem documentação inline e docstrings

---

## 📝 Exemplos Completos

### Exemplo 1: Executar série temporal do Brasil (NM)

```bash
# Windows PowerShell
cd c:\Users\danilo.giomo\documents\comex
python -m NM.serie_temporal.COMEX_BRA.COMEX_BRA_NM
```

### Exemplo 2: Executar Globinho da Argentina

```bash
# Windows PowerShell
cd c:\Users\danilo.giomo\documents\comex
python -m NM.globinho.ARG.COMEX_ARG_GLOBE
```

### Exemplo 3: Executar orquestrador europeu

```bash
# Windows PowerShell
cd c:\Users\danilo.giomo\documents\comex
cd NM\serie_temporal
python orquestrador_europa_NM.py
```

### Exemplo 4: Executar metodologia antiga do Brasil

```bash
# Windows PowerShell
cd c:\Users\danilo.giomo\documents\comex
python -m OM.COMEX_BRA.COMEX_BRA
```

---