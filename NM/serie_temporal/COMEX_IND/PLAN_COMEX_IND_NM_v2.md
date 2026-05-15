# Implementation Plan: India (IND) — Comex NM v2

**Date:** 2026-05-11  
**Author:** the-planner + mario.carvalho1  
**Status:** Approved — ready for implementation

---

## Context

Integrating India (IND) into the Comex Nova Metodologia v2 pipeline at `Comex-Derik\NM\serie_temporal\`. A working scraper with 62 months of historical data (Jan 2021 → Feb 2026) already exists at `Mario\Comex\COMEX_IND\`.

---

## All Decisions Resolved

| Decision | Answer |
|---|---|
| Country name in IDS | `"Índia"` (with accent, must match exactly) |
| pais_id mapping | Portal `country_id` → `pais_id` directly (no mapping file, same pattern as BRA) |
| Reference file for IDs | `Comex-Derik\library\IDS_comex.xlsx` (Pais_1 = "Índia", already registered) |
| Proxies in production | Keep proxy support (speeds up scraping) |
| Date / lookback | No lookback — collect only the newest available month each run |
| CIF data | `frete = NaN`, `seguro = NaN`, `Valor_Cif = NaN` (source does not provide CIF) |
| Value unit | `value_usd_million × 1,000,000` → raw USD in `normalize_columns()` |
| Quantity unit | Assumed KG for tracked NCMs (portal does not report unit per row; scraper `quantity_unit` is null) |
| Country-code mapping file | **Not needed** — portal `country_id` used as `pais_id` directly |

---

## Scraper → Canonical Column Mapping

| Scraper column | Canonical column | Transformation |
|---|---|---|
| `hs_code` (str, zero-padded) | `ncm` (int) | `pd.to_numeric(hs_code)` — strips leading zeros |
| `country_id` (int16) | `pais_id` (int) | Direct cast |
| `country_name` (str) | `pais_name` (str) | Direct |
| `quantity` (float32) | `peso` (float) | Direct (assumed KG) |
| `value_usd_million × 1e6` | `valor` (float64) | Multiply in `normalize_columns()` |
| `year` + `month` | `Data` (datetime) | `datetime(year, month, 1)` |
| `trade_flow` ("IMPORT"/"EXPORT") | `ImportExport` ("Import"/"Export") | String map |
| — | `frete` | `NaN` |
| — | `seguro` | `NaN` |

---

## Target Directory Structure

```
Comex-Derik\NM\
├── serie_temporal\
│   └── COMEX_IND\
│       ├── __init__.py
│       ├── COMEX_IND_NM_v2.py            ← pipeline class (Phase 3)
│       ├── bootstrap_historical.py        ← one-shot historical bootstrap (Phase 4)
│       ├── PLAN_COMEX_IND_NM_v2.md        ← this file
│       └── scraper\                       ← MIGRATED from Mario\Comex\COMEX_IND\scraper\
│           ├── __init__.py
│           ├── constants.py              ← EDITED: new PARQUET_BASE, LOGS_DIR, PROXIES_CSV
│           ├── session_manager.py
│           ├── proxy_manager.py
│           ├── fetcher.py
│           ├── month_worker.py
│           ├── checkpoint.py
│           ├── orchestrator.py           ← EDITED: expose run_range() for in-process calls
│           └── tests\
│
└── dados\
    └── IND\
        ├── scraper_parquet\              ← MIGRATED 62 monthly parquets
        │   └── year=YYYY\month=MM\meidb_YYYY_MM.parquet
        ├── database\
        │   └── historical.parquet        ← Phase 4 output
        ├── raw\
        ├── silver\
        ├── gold\
        ├── cache\
        └── proxies.csv                   ← MIGRATED from Mario\Comex\proxies.csv
```

**Files to edit:**
- `serie_temporal\data-contract.yaml` — fix IND section (lines 264–276)

**Files NOT to modify:**
- `costdrivers_comex_NM_v2.py` — base class, do not touch
- `nm_config.py`, `nm_filters.py`, `nm_reasons.py`

---

## Phase 1 — Scraper Migration

Move scraper code and data from `Mario\Comex\COMEX_IND\` into the NM tree.

### Steps

1. Copy scraper code:
   ```
   robocopy "Mario\Comex\COMEX_IND\scraper" "Comex-Derik\NM\serie_temporal\COMEX_IND\scraper" /E /XD __pycache__
   ```

2. Copy 62 monthly parquets:
   ```
   robocopy "Mario\Comex\COMEX_IND\data\parquet" "Comex-Derik\NM\dados\IND\scraper_parquet" /E
   ```

3. Copy proxies:
   ```
   Copy-Item "Mario\Comex\proxies.csv" "Comex-Derik\NM\dados\IND\proxies.csv"
   ```

4. Edit `scraper\constants.py` — repoint path constants:
   ```python
   _PKG_ROOT = Path(__file__).resolve().parent
   _IND_DATA_ROOT = _PKG_ROOT.parent.parent.parent.parent / "dados" / "IND"
   PARQUET_BASE = _IND_DATA_ROOT / "scraper_parquet"
   LOGS_DIR     = _IND_DATA_ROOT / "logs"
   PROXIES_CSV  = _IND_DATA_ROOT / "proxies.csv"
   ```

5. Run scraper tests to confirm relocation is clean.

### Validation gate
- All 62 parquets present under `dados\IND\scraper_parquet\year=*\month=*\`
- Scraper tests pass
- `python -m orchestrator` exits cleanly (all months already complete)

---

## Phase 2 — Fix `data-contract.yaml`

Replace lines 264–276 (current placeholder IND section) with real scraper column names.

```yaml
IND:
  export:
    hs_code: ncm
    country_id: pais_id
    country_name: pais_name
    quantity: peso
    valor_raw_usd: valor
    columns: ['Data', 'ncm', 'ImportExport', 'pais_id', 'pais_name', 'peso', 'valor', 'frete', 'seguro']
  import:
    hs_code: ncm
    country_id: pais_id
    country_name: pais_name
    quantity: peso
    valor_raw_usd: valor
    columns: ['Data', 'ncm', 'ImportExport', 'pais_id', 'pais_name', 'peso', 'valor', 'frete', 'seguro']
```

Note: `valor_raw_usd` is an intermediate column computed in `normalize_columns()` before the YAML rename is applied.

---

## Phase 3 — Pipeline Class (`COMEX_IND_NM_v2.py`)

Subclasses `ComexPipelineNMv2`. Implements 3 abstract methods + 1 optional.

### Skeleton

```python
class COMEX_IND_NM_v2(ComexPipelineNMv2):

    def __init__(self, config=None, start_date=None, use_azure=True, developing=False):
        super().__init__(
            iso_code="IND",
            iso_database="IND",
            config=config,
            start_date=start_date,
            data_contract_path="data-contract.yaml",
            ids_table_path="IDS_comex.xlsx",
            use_azure=use_azure,
            developing=developing,
        )
        self._scraper_parquet_root = ...  # NM/dados/IND/scraper_parquet

    def _get_country_name(self) -> str:
        return "Índia"

    def collect_import_data(self) -> pd.DataFrame:
        return self._collect_trade("IMPORT")

    def collect_export_data(self) -> pd.DataFrame:
        return self._collect_trade("EXPORT")

    def _collect_trade(self, flow: str) -> pd.DataFrame:
        # 1. Find latest scraped month in scraper_parquet/
        # 2. Call orchestrator.run_range() for the newest missing month only
        # 3. Read all parquets within the pipeline's date window via PyArrow predicate pushdown
        # 4. Filter by trade_flow == flow
        # 5. Return raw DataFrame

    def normalize_columns(self, df, contract, import_export) -> pd.DataFrame:
        df = df.copy()
        df["valor_raw_usd"] = pd.to_numeric(df["value_usd_million"], errors="coerce") * 1_000_000
        df["Data"] = pd.to_datetime(
            df["year"].astype(int) * 10000 + df["month"].astype(int) * 100 + 1,
            format="%Y%m%d",
        )
        df["hs_code"] = pd.to_numeric(df["hs_code"], errors="coerce").astype("Int64")
        df["ImportExport"] = df["trade_flow"].astype(str).str.capitalize().str.replace("Import", "Import").str.replace("Export", "Export")
        df["frete"] = float("nan")
        df["seguro"] = float("nan")
        return super().normalize_columns(df, contract, import_export)

    def _country_specific_treatment(self, df: pd.DataFrame) -> pd.DataFrame:
        # type coercions, drop intermediate columns
        return df
```

### Scraper refactor needed
`orchestrator.py` must expose a `run_range(months: list[tuple[int,int]])` function callable without `if __name__ == "__main__"` — allows in-process invocation from `_collect_trade()` without subprocess overhead.

---

## Phase 4 — Historical Bootstrap (`bootstrap_historical.py`)

One-shot script to populate `dados\IND\database\historical.parquet` from all 62 existing scraper parquets.

### Algorithm
1. Instantiate `COMEX_IND_NM_v2(developing=True)`
2. Read all parquets from `dados/IND/scraper_parquet/` (all 62 months, both trade flows)
3. Split by `trade_flow`, pass each through `normalize_columns()` + `_country_specific_treatment()`
4. Concatenate and write to `historical.parquet` via the base class writer

### Validation gate
- 62 distinct months (2021-01-01 → 2026-02-01)
- Zero nulls in `ncm`, `pais_id`, `Data`, `ImportExport`
- `valor` values are positive and in plausible USD range (billions/month total)

---

## Phase 5 — Integration Testing (`developing=True`)

```python
pipeline = COMEX_IND_NM_v2(developing=True)
pipeline.run(skip_phases=["upload"])
```

Inspect outputs at `dados\IND\`:
- `silver\silver_{date}.parquet` — data that passed all 5 filters
- `silver\dropped_{date}.parquet` — audit log with drop reasons
- `gold\gold_NM_v2_{date}.parquet` — final `(ID, Data, Valor, Valor_Cif)`

Validation:
- Gold has rows for every tracked NCM × month
- `Valor = valor / peso` (USD/kg) is plausible per commodity
- `Valor_Cif` is NaN (expected — no CIF source)
- Check `dropped.parquet` for unexpected `ncm_not_mapped` or `insufficient_history` drops

---

## Phase 6 — Production Cutover

1. Enable upload: `COMEX_IND_NM_v2(developing=False, config=NMConfig(allow_upload=True))`
2. Schedule monthly run (cadence TBD by user)
3. Monitor first 2 cycles before considering integration complete

---

## Known Risks

| Risk | Severity | Mitigation |
|---|---|---|
| `quantity_unit` is null in scraper — KG assumed for tracked NCMs | Medium | Phase 5 USD/kg ratio sanity check per NCM |
| Portal `country_id` scheme could change upstream | Low | Parquets are immutable snapshots; only affects future months |
| Proxy pool exhausted at run time | Medium | Surface clear error; monthly cadence allows manual rerun |
| `value_usd_million` overflow if cast to float32 | Low | Cast to float64 before multiplying in `normalize_columns()` |
