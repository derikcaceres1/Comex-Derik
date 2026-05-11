# =============================================================
# run_all.ps1 — Executa todos os testes do pipeline NM v2
#
# Uso (a partir de qualquer diretório):
#   powershell -ExecutionPolicy Bypass -File "caminho\para\run_all.ps1"
#
# O que faz:
#   1. Re-executa o pipeline BRA v2 (skip collect/upload) e imprime resumo
#   2. Re-executa o pipeline ITA v2 (skip collect/upload) e imprime resumo
#   3. Compara gold v1 vs v2 para BRA
#   4. Compara gold v1 vs v2 para ITA
#
# Pré-requisitos:
#   • historical.parquet de BRA em NM/dados/BRA/database/
#   • historical.parquet de EUR em NM/dados/EUR/database/
#   • gold_NM_*.parquet de v1 em NM/dados/BRA/gold/ e NM/dados/ITA/gold/
# =============================================================

$ErrorActionPreference = "Stop"
$TESTS_DIR = $PSScriptRoot

Write-Host ""
Write-Host "============================================================"
Write-Host "  PIPELINE NM v2 — RUN ALL TESTS"
Write-Host "============================================================"
Write-Host ""

# ---- BRA: rerun ----
Write-Host ">>> [1/4] Re-executando pipeline BRA v2..."
Write-Host ""
python "$TESTS_DIR\BRA\rerun_calc_v2.py"
if ($LASTEXITCODE -ne 0) { Write-Host "[ERRO] BRA rerun falhou."; exit 1 }

Write-Host ""
Write-Host "------------------------------------------------------------"

# ---- ITA: rerun ----
Write-Host ""
Write-Host ">>> [2/4] Re-executando pipeline ITA v2..."
Write-Host ""
python "$TESTS_DIR\ITA\rerun_calc_v2.py"
if ($LASTEXITCODE -ne 0) { Write-Host "[ERRO] ITA rerun falhou."; exit 1 }

Write-Host ""
Write-Host "------------------------------------------------------------"

# ---- BRA: comparação ----
Write-Host ""
Write-Host ">>> [3/4] Comparando gold v1 vs v2 — BRA..."
Write-Host ""
python "$TESTS_DIR\BRA\comparar_gold_v1_v2.py"
if ($LASTEXITCODE -ne 0) { Write-Host "[ERRO] BRA comparação falhou."; exit 1 }

Write-Host ""
Write-Host "------------------------------------------------------------"

# ---- ITA: comparação ----
Write-Host ""
Write-Host ">>> [4/4] Comparando gold v1 vs v2 — ITA..."
Write-Host ""
python "$TESTS_DIR\ITA\comparar_gold_v1_v2_ita.py"
if ($LASTEXITCODE -ne 0) { Write-Host "[ERRO] ITA comparação falhou."; exit 1 }

Write-Host ""
Write-Host "============================================================"
Write-Host "  TODOS OS TESTES CONCLUIDOS COM SUCESSO"
Write-Host "============================================================"
Write-Host ""
