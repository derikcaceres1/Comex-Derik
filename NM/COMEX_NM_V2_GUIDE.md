# Comex NM v2 — New Country Onboarding Guide

**Scope:** This document provides the complete context needed to integrate a brand-new country into the Comex Nova Metodologia (NM) v2 pipeline, starting from nothing but a working scraper and raw data.

**What this document is NOT:** A migration guide for existing v1 countries (ARG, COL, JPN, EU). That is future work.

---

## Table of Contents

1. [System Overview](#1-system-overview)
2. [Architecture: What the Base Class Gives You for Free](#2-architecture-what-the-base-class-gives-you-for-free)
3. [The Developer Contract: What You Must Implement](#3-the-developer-contract-what-you-must-implement)
4. [Data Contract: The Required Schema](#4-data-contract-the-required-schema)
5. [IDS Table: Registering Your Country's Indicators](#5-ids-table-registering-your-countrys-indicators)
6. [Step-by-Step: Onboarding a New Country](#6-step-by-step-onboarding-a-new-country)
7. [Configuration Reference (NMConfig)](#7-configuration-reference-nmconfig)
8. [Statistical Methodology Reference](#8-statistical-methodology-reference)
9. [Folder and File Naming Conventions](#9-folder-and-file-naming-conventions)
10. [Testing Without Uploading](#10-testing-without-uploading)
11. [Key Open Questions for Every New Country](#11-key-open-questions-for-every-new-country)
12. [Pain Points to Avoid in New Implementations](#12-pain-points-to-avoid-in-new-implementations)
13. [Existing Country Implementations as Reference](#13-existing-country-implementations-as-reference)

---

## 1. System Overview

### What This System Does

The Comex NM pipeline computes monthly **import/export price indices** (FOB/weight and CIF/weight ratios) for a set of product codes (NCMs) across multiple countries, then uploads these values to the **CostDrivers platform** (`https://api-costdrivers.gep.com/costdrivers-api`). These values are consumed by the platform as cost-driver inputs for procurement and contract management.

Each index on the platform is identified by an `IDIndicePrincipal`. There is one indicator per `(NCM, ImportExport, country)` combination. NM-methodology indicators all have `IDIndicePrincipal >= 382958`.

### The Three-Step Business Logic

```
1. COLLECT    Raw trade data from official government source (web scraping / download)
       ↓
2. CALCULATE  Statistical cleaning pipeline:
              raw monthly data → filter → STL outlier detection → IQR outlier correction → interpolation → price index
       ↓
3. UPLOAD     Push monthly index values (Valor, Valor_Cif) to CostDrivers API
```

### What "Nova Metodologia" Means

The NM (Nova Metodologia) refers to a specific statistical pipeline applied to the raw trade data before uploading index values. It involves:
- **Filtering** bad/sparse series before any calculation
- **STL decomposition** to detect statistical outliers in residuals
- **IQR correction** as a second outlier-removal pass
- **Linear interpolation** to fill gaps introduced by outlier removal
- A set of **guardrails** for edge cases (negative values, the most recent month, trailing gaps)

The methodology and its parameters are fully encapsulated in the base class. A new country implementation does NOT need to re-implement any of this.

### Technology Stack

| Component | Technology |
|---|---|
| Primary data library | `pandas` |
| Statistical processing | `statsmodels` (STL decomposition) |
| HTTP scraping | `requests`, `beautifulsoup4` |
| API calls (async) | `library.costdrivers.ApiAsync` (internal, at `Comex-Derik/library/`) |
| Cloud storage (production) | Azure ADLS2 via `BlobStorage_API.AzureBlobStorage` (internal) |
| Intermediate storage | Parquet files |
| Configuration | Python dataclass (`NMConfig`), YAML (`data-contract.yaml`) |

---

## 2. Architecture: What the Base Class Gives You for Free

**File:** `serie_temporal/costdrivers_comex_NM_v2.py`  
**Class:** `ComexPipelineNMv2`

Once you implement the required abstract methods (see Section 3), the base class handles the entire pipeline automatically through six phases:

```
run()
  ├── collect()              ← calls your abstract methods
  ├── update_historical()    ← appends raw data to historical.parquet
  ├── normalize_historical() ← applies data-contract column mapping
  ├── filter_data()          ← 5 filters: mapped, min history, max gap, top%, invalid rows
  ├── calculate()            ← STL + IQR + interpolation per IDIndicePrincipal
  └── upload()               ← pushes Valor + Valor_Cif to CostDrivers API
```

### Phase Details

**`collect()`**
- Calls `collect_import_data()` and `collect_export_data()` (your abstract methods)
- Saves raw DataFrames to `NM/dados/{ISO_CODE}/raw/import_raw_{date}.parquet` and `export_raw_{date}.parquet`
- Cache: avoids re-downloading if a same-day parquet already exists in `NM/dados/{ISO_CODE}/cache/`

**`update_historical()`**
- Loads the existing `NM/dados/{ISO_CODE}/database/historical.parquet`
- **Append-only**: never overwrites existing records; adds only rows whose `(Data, ncm, pais_id, ImportExport)` key is not already present
- Safety guard: if updated size < existing size, preserves existing (bug protection)

**`normalize_historical()`**
- Splits historical DataFrame by `ImportExport` (`== 0` and `== 1`, integers), then calls `normalize_columns()` on each half
- Produces a unified DataFrame with the canonical schema (see Section 4)
- **Double-call warning:** if `historical.parquet` was bootstrapped by a script that already called `normalize_columns()`, the stored columns are already canonical (`valor`, `peso`, `ncm`, etc.). When `normalize_historical` calls `normalize_columns()` again, the raw column names no longer exist. Any filter inside `normalize_columns()` that only checks pre-rename names will silently skip. Always check both raw and canonical column names in your filters (see Section 3.3).

**`filter_data()` — 5 Filters**

| Filter | ID | Drops | Config param |
|---|---|---|---|
| NCM not in IDS table | F1 | Series with no matching `IDIndicePrincipal` | N/A |
| Insufficient history | F2 | `IDIndicePrincipal` with < 24 distinct months | `min_months_required` |
| Large internal gap | F3 | IDs where max consecutive gap > 3 months | `max_internal_gap_months` |
| Top 80% countries | F4 | Countries below cumulative 80% weight threshold | `top_percent_threshold` |
| Invalid rows | F5 | Rows with null or non-positive `peso` or `valor` | N/A |

**Important:** F4 (`top_n_percent`) raises a hard `ValueError` if any null or zero values exist in `peso` or `valor` — it runs before F5 and does not tolerate them. Do not rely on F5 to clean these rows. Drop `peso <= 0` and `valor <= 0` inside `normalize_columns()` before they reach the filter phase (see Section 3.3).

All dropped series are written to `silver/dropped_{date}.parquet` with a reason column for auditability.

**`calculate()`**
- Groups data by `IDIndicePrincipal`
- Computes `alpha = CIF_80 / FOB_80` (CIF-to-FOB ratio; will be NaN if source has no CIF data)
- Runs preprocessing → STL outlier detection → IQR outlier correction → interpolation
- Output: `gold_NM_v2_{date}.parquet` with columns `(ID, Data, Valor, Valor_Cif)`

**`upload()`**
- Blocked in `developing=True` mode
- Fetches the last 12 months from the platform to determine what already exists
- Only uploads rows with `Data >= max_date_already_on_platform` per ID
- Uses `ApiAsync` to PUT to `/api/v1/DataScience/UpdateOption-9`

---

## 3. The Developer Contract: What You Must Implement

When subclassing `ComexPipelineNMv2`, you **must** implement three abstract methods:

### 3.1 `collect_import_data() → pd.DataFrame`

Downloads/scrapes the import data from the official source. Must return a raw DataFrame. Column names at this stage do not need to match the canonical schema — normalization happens in `normalize_columns()`.

```python
def collect_import_data(self) -> pd.DataFrame:
    # Download from official source
    # Return raw DataFrame (any column names)
    ...
```

### 3.2 `collect_export_data() → pd.DataFrame`

Same as above for export data.

```python
def collect_export_data(self) -> pd.DataFrame:
    # Download from official source
    # Return raw DataFrame (any column names)
    ...
```

### 3.3 `normalize_columns(df, contract, import_export) → pd.DataFrame`

Maps raw columns to the canonical schema and applies data-quality guards before passing control to the base class.

**Actual signature:**
```python
def normalize_columns(self, df: pd.DataFrame, contract: dict, import_export: int) -> pd.DataFrame:
```
- `contract` — the loaded data-contract YAML dict (passed in by the base class; do not load it yourself)
- `import_export` — `1` for import, `0` for export (integer, not string)

Must end with `return super().normalize_columns(work, contract, import_export)`, which applies the YAML column renaming and column selection defined in `data-contract.yaml`.

**Critical — this method is called twice on the same country's data:**
1. During `collect()` on freshly scraped data — columns have raw scraper names
2. By `normalize_historical()` on data loaded from `historical.parquet` — columns may already be canonical if the parquet was bootstrapped by a script that already called `normalize_columns()`

Filters inside this method must handle both forms. Check raw column names first, then fall back to canonical names. Pattern for zero/null guards:

```python
def normalize_columns(self, df: pd.DataFrame, contract: dict, import_export: int) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    work = df.copy()

    # Compute intermediate columns when raw names are present
    if "raw_value_col" in work.columns:
        work["valor_raw_usd"] = pd.to_numeric(work["raw_value_col"], errors="coerce") * 1_000_000

    # Build Data column, set frete/seguro = NaN, etc.
    # ...

    # ImportExport MUST be integer: 1 = import, 0 = export
    work["ImportExport"] = import_export

    # Drop zero/null peso and valor HERE — F4 (top_n_percent) crashes on them before F5 runs.
    # Check raw name first, fall back to canonical name (handles both call sites).
    valor_col = next((c for c in ("valor_raw_usd", "valor") if c in work.columns), None)
    if valor_col:
        work = work.loc[pd.to_numeric(work[valor_col], errors="coerce").fillna(0) > 0].copy()

    peso_col = next((c for c in ("raw_weight_col", "peso") if c in work.columns), None)
    if peso_col:
        work = work.loc[pd.to_numeric(work[peso_col], errors="coerce").fillna(0) > 0].copy()

    return super().normalize_columns(work, contract, import_export)
```

### 3.4 `_get_country_name() → str` (required — abstract method)

Returns the country name as it appears in `IDS_comex.xlsx` (`Pais_1` column). Used to filter the IDS table to this country's indicators. Skipping this raises `TypeError: Can't instantiate abstract class` at runtime.

```python
def _get_country_name(self) -> str:
    return "Mexico"  # must match exactly the value in IDS_comex.xlsx
```

---

## 4. Data Contract: The Required Schema

After `normalize_columns()`, your DataFrame must have exactly these columns:

| Column | Type | Description |
|---|---|---|
| `Data` | `datetime` | Month of the record (`YYYY-MM-01` format) |
| `ncm` | `int` | NCM product code (numeric, 8 digits) |
| `pais_id` | `int` | Country code from the **source country's** coding system (will be mapped to `IDNCMPais` later) |
| `peso` | `float` | Net weight in kg |
| `valor` | `float` | FOB value (or CIF if FOB is not available from source) |
| `frete` | `float` | Freight value (set to `NaN` if not available) |
| `seguro` | `float` | Insurance value (set to `NaN` if not available) |
| `ImportExport` | `int` | `1` for import, `0` for export |

**Notes:**
- `frete` and `seguro` are optional. If the data source does not provide them, set to `NaN`. The pipeline will compute `Valor_Cif = NaN` for this country (the CIF projection SQL script can fill these later for specific countries).
- `ncm` must be numeric (int). Strip any leading zeros before storing.
- `Data` must be `datetime64` (pandas). Use the first day of the month (`day=1`).
- `pais_id` is the **source country code** (e.g., a country code from the official statistics bureau). It does NOT need to be the CostDrivers internal `IDNCMPais` yet — the pipeline handles this mapping via the country-specific lookup files.
- `ImportExport` must be an integer (`1`/`0`), not a string. The base class `normalize_historical()` splits the historical DataFrame by `ImportExport == 0` and `== 1`. Storing strings causes the split to produce empty DataFrames silently, resulting in an empty normalized historical.

### How Canonical Columns Are Declared

The `data-contract.yaml` file at `serie_temporal/data-contract.yaml` defines the column mapping for each country × direction. When you add a new country, you must add a section for it in this file:

The format is `raw_column_name: canonical_column_name`. The special `columns` key lists which canonical columns to keep after renaming. Keys in the mapping that do not exist in the DataFrame are silently skipped — the rename is non-strict.

```yaml
# Example entry for a new country (replace XXX with ISO code)
XXX:
  export:
    raw_ncm_col: ncm               # raw column name → canonical name
    raw_country_col: pais_id
    raw_country_name_col: pais_name
    raw_weight_col: peso
    valor_raw_usd: valor           # intermediate col computed in normalize_columns()
    columns: ['Data', 'ncm', 'ImportExport', 'pais_id', 'pais_name', 'peso', 'valor', 'frete', 'seguro']
  import:
    raw_ncm_col: ncm
    raw_country_col: pais_id
    raw_country_name_col: pais_name
    raw_weight_col: peso
    valor_raw_usd: valor
    columns: ['Data', 'ncm', 'ImportExport', 'pais_id', 'pais_name', 'peso', 'valor', 'frete', 'seguro']
```

Note: `valor_raw_usd` is a common pattern — compute it in `normalize_columns()` as `raw_value * scale_factor`, then let the contract rename it to `valor`. This keeps the multiply-and-rename logic explicit and testable.

---

## 5. IDS Table: Registering Your Country's Indicators

**File:** `library/IDS_comex.xlsx`

This is the **single most critical configuration asset** in the entire system. It is the master mapping between `(country, NCM, ImportExport)` and the platform's `IDIndicePrincipal`. Without entries for your new country in this file, **none of the data will be uploaded** (Filter F1 will drop everything).

### Schema

| Column | Description |
|---|---|
| `Pais_1` | Country name (must match `_get_country_name()`) |
| `IDIndicePrincipal` | Platform indicator ID (>= 382958 for NM methodology; see exception below) |
| `NCM` | NCM product code |
| `ImportExport` | `"Import"` or `"Export"` |

### How to Register a New Country

1. Identify the list of NCMs this country will track (Import + Export)
2. For each `(NCM, ImportExport)` combination, a new `IDIndicePrincipal` must be created in the CostDrivers platform
3. All IDs created for the NM methodology will be `>= 382958` (this is the NM threshold)
4. Add all `(Pais_1, IDIndicePrincipal, NCM, ImportExport)` rows to `IDS_comex.xlsx`
5. The `Pais_1` value must be an exact string match to what `_get_country_name()` returns

**Who creates new indicator IDs?** This must be clarified with the CostDrivers platform team. The process for registering new `IDIndicePrincipal` values in the platform is external to this codebase.

### Exception: Countries with OM-Era IDs Migrating to NM

Some countries were originally tracked under the Old Methodology (OM) and have existing `IDIndicePrincipal` values below 382958. When these countries are integrated into the NM pipeline without re-creating their IDs, the base class `load_ids_table()` filters them all out (it applies the `>= 382958` threshold for all non-EUR countries), returning 0 records and silently dropping everything at F1.

**Fix:** override `load_ids_table()` in the country subclass to skip the threshold:

```python
def load_ids_table(self) -> pd.DataFrame:
    # This country uses OM-era IDs (all below 382958) — bypass the NM threshold.
    library_path = Path("library/IDS_comex.xlsx")
    if not library_path.exists():
        raise FileNotFoundError(f"IDS_comex.xlsx not found: {library_path.absolute()}")

    ids = pd.read_excel(library_path)
    country_name = self._get_country_name()
    ids = ids[ids["Pais_1"] == country_name].copy()
    if ids.empty:
        self.logger.warning("No IDs found for '%s'", country_name)
        return pd.DataFrame(columns=["NCM", "ImportExport", "IDIndicePrincipal"])

    ids["NCM"] = pd.to_numeric(ids["NCM"], errors="coerce").astype("Int64")
    ids["ImportExport"] = pd.to_numeric(ids["ImportExport"], errors="coerce").astype("Int64")
    ids["IDIndicePrincipal"] = pd.to_numeric(ids["IDIndicePrincipal"], errors="coerce").astype("Int64")
    ids = ids.dropna(subset=["NCM", "ImportExport", "IDIndicePrincipal"])
    ids = ids[["NCM", "ImportExport", "IDIndicePrincipal"]].copy()
    ids.reset_index(drop=True, inplace=True)
    self.logger.info("IDs carregados: %d registros para %s", len(ids), country_name)
    return ids
```

**Symptom to watch for:** log line `IDs carregados: 0 registros para {country}` immediately after `Filtrando IDs para país`.

---

## 6. Step-by-Step: Onboarding a New Country

### Prerequisites

- [ ] A working scraper that can collect import and/or export data from the official source
- [ ] New indicator IDs (`IDIndicePrincipal`) registered in the CostDrivers platform for this country's NCMs
- [ ] `IDS_comex.xlsx` updated with the new country's entries
- [ ] A country-code mapping file (maps source country codes → platform `IDNCMPais`)
- [ ] At least 24 months of historical raw data available (Filter F2 requires this minimum)

### Step 1: Create the country directory

```
serie_temporal/
└── COMEX_{XXX}/           ← use 3-letter ISO code
    ├── __init__.py
    └── COMEX_{XXX}_NM_v2.py
```

### Step 2: Write the pipeline class

```python
# serie_temporal/COMEX_{XXX}/COMEX_{XXX}_NM_v2.py

import pandas as pd
from serie_temporal.costdrivers_comex_NM_v2 import ComexPipelineNMv2
from serie_temporal.nm_config import NMConfig


class ComexXXXPipelineNMv2(ComexPipelineNMv2):

    def __init__(self, **kwargs):
        super().__init__(iso_code="XXX", **kwargs)

    def _get_country_name(self) -> str:
        return "CountryName"  # must match Pais_1 in IDS_comex.xlsx exactly

    def collect_import_data(self) -> pd.DataFrame:
        # scraping logic here
        return df

    def collect_export_data(self) -> pd.DataFrame:
        # scraping logic here
        return df

    def normalize_columns(self, df: pd.DataFrame, ie: str) -> pd.DataFrame:
        # rename and compute canonical columns
        return df[["Data", "ncm", "pais_id", "peso", "valor", "frete", "seguro", "ImportExport"]]
```

### Step 3: Add your country to `data-contract.yaml`

Add a `XXX` section with the Import and Export column mappings. See Section 4 for the format.

### Step 4: Create the country-code mapping file

The `normalize_columns()` method may need a country-code lookup file to map `pais_id` (source system codes) to `IDNCMPais` (platform codes). Common approach used by existing countries:

- CSV file at `NM/dados/{XXX}/paisID_{XXX}.csv` with columns `source_country_code, IDNCMPais`
- Or an Excel file cross-referenced against `tblNCM_Pais.xlsx` (already in the repo)

Check `tblNCM_Pais.xlsx` first — it contains platform country IDs by multiple naming/coding schemes (`Pais_1`, `Pais_2`, `Pais_3`, ISO codes, UN M49 codes). Your source may already use a scheme that maps directly.

### Step 5: Bootstrap historical data

The pipeline **appends only** to `historical.parquet` — it never backfills. Before running the pipeline for the first time, you must bootstrap the historical file with at least 24 months of data (Filter F2 minimum).

Option A — Run the scraper backwards in time for each past month and build the parquet manually.

Option B — Use `collect()` with an early `start_date` in `ComexPipelineNMv2.__init__()` (e.g., `start_date="2023-01-01"`) and run the collect + update_historical phases before the first full run.

Option C — Write a standalone `bootstrap_historical.py` script that reads all existing scraped monthly parquets, calls `normalize_columns()` on each, and writes the result directly to `NM/dados/{XXX}/database/historical.parquet`. This is the recommended approach when a scraper already has years of data in Hive-partitioned parquets (`year=YYYY/month=MM/`).

**Important for Option C:** the bootstrap script calls `normalize_columns()` and writes canonical column names (`valor`, `peso`, `ncm`, etc.) to the parquet. When the pipeline later runs `normalize_historical()`, it calls `normalize_columns()` again on that parquet. Because the columns are already canonical, the raw-name filters inside `normalize_columns()` will silently skip. Ensure your filters check both raw and canonical column names (see Section 3.3).

The historical parquet lives at: `NM/dados/{XXX}/database/historical.parquet`

### Step 6: Validate in `developing` mode

Run the full pipeline with `developing=True` and `allow_upload=False` (the defaults). This uses local filesystem only, disables Azure, and does **not** push anything to the platform.

```python
pipeline = ComexXXXPipelineNMv2(developing=True)
pipeline.run()
```

Inspect the output files:
- `NM/dados/{XXX}/silver/silver_{date}.parquet` — data that passed all filters
- `NM/dados/{XXX}/silver/dropped_{date}.parquet` — data dropped by each filter, with reason
- `NM/dados/{XXX}/gold/gold_NM_v2_{date}.parquet` — final index values `(ID, Data, Valor, Valor_Cif)`

### Step 7: Inspect the dropped audit log

The `dropped_{date}.parquet` has columns `(IDIndicePrincipal, ncm, ImportExport, reason, detail)`. Common reasons and what to investigate:

| Reason | Cause | Action |
|---|---|---|
| `ncm_not_mapped` | NCM exists in raw data but not in `IDS_comex.xlsx` | Check if the ID is registered in the platform and add to IDS table |
| `insufficient_history` | Fewer than 24 months for this ID | Bootstrap more historical data |
| `large_gap` | A gap > 3 consecutive months in the series | Investigate if data is truly missing or a scraper bug |
| `below_top_percent` | Country's weight is below the 80% threshold | Expected; those countries are aggregated into "others" |
| `invalid_row` | Zero or null `peso`/`valor` | Data quality issue in source |

### Step 8: Enable upload for production

Once the gold output looks correct, run with upload enabled:

```python
from serie_temporal.nm_config import NMConfig

config = NMConfig(allow_upload=True)
pipeline = ComexXXXPipelineNMv2(developing=False, config=config)
pipeline.run()
```

---

## 7. Configuration Reference (NMConfig)

**File:** `serie_temporal/nm_config.py`  
**Class:** `NMConfig` (Python dataclass, all fields have defaults)

| Parameter | Default | Description |
|---|---|---|
| `min_months_required` | `24` | Filter F2: minimum number of distinct months a series must have |
| `max_internal_gap_months` | `3` | Filter F3: maximum consecutive missing months allowed |
| `top_percent_threshold` | `0.8` | Filter F4: keep top countries summing to this % of total weight |
| `max_internal_interpolation` | `3` | Max interior NaN values that can be linearly interpolated |
| `max_tail_extrapolation` | `3` | Max trailing NaN values filled by rolling mean |
| `stl_seasonal_period` | `13` | Declared seasonal period (NOTE: internal calculation uses 12) |
| `stl_outlier_zscore` | `2.5` | Residual z-score threshold to flag a data point as STL outlier |
| `historic_cutoff_date` | `"2022-01-01"` | Data before this date is "historic" — never replaced by new data |
| `revise_published_months` | `False` | If True, allows overwriting already-uploaded values |
| `hold_suspicious_outliers` | `True` | If True, applies extra guardrails on the most recent month |
| `overwrite_recent_history` | `False` | If True, overwrites recent months in historical.parquet (use with care) |
| `allow_upload` | `False` | Safety gate: must be set to True explicitly to enable upload |
| `save_dropped_dataframe` | `True` | If True, saves the dropped audit log to silver/ folder |

Per-country customization: instantiate `NMConfig` with any overrides and pass it to the pipeline constructor.

```python
# Example: lower the history requirement for a country with sparse data
config = NMConfig(min_months_required=12, allow_upload=True)
pipeline = ComexXXXPipelineNMv2(config=config)
```

---

## 8. Statistical Methodology Reference

This section documents the NM statistical pipeline at a conceptual level. It is implemented in `costdrivers_comex_NM_v2.py` in the `calculate()` method.

### Input

`silver_df` grouped by `IDIndicePrincipal`. Each group is a monthly time series of `(Data, FOB_80, CIF_80)` values, where:
- `FOB_80` = sum of FOB values for the top countries covering 80% of trade weight
- `CIF_80` = same for CIF values (NaN if source has no CIF data)

### Pipeline per `IDIndicePrincipal`

```
1. COMPUTE ALPHA
   alpha = CIF_80 / FOB_80  per (IDIndicePrincipal, Data) where CIF_80 > 0

2. PREPROCESS FOB_80 SERIES
   a. reindex_series    → fill date gaps, replace 0 with NaN
   b. fill_tail_nan     → fill trailing NaN with 3-month rolling mean
   c. interpolate_series → linear interpolation for interior NaN

3. STL OUTLIER DETECTION (residual z-score)
   - STL decomposition with seasonal=12, robust=True
   - Compute residuals; z-score normalize them
   - If |z| > stl_outlier_zscore (default 2.5): mark as outlier, set Valor_clean = NaN
   - Fill outlier positions with rolling mean of last 3 non-outlier residuals
   - Reconstruct: Valor_clean = trend + seasonality + smoothed_residuals

4. FILTER TO TARGET PERIOD
   - Only process data from historic_cutoff_date (2022-01-01) forward
   - Earlier data was used only for STL fitting

5. IQR OUTLIER CORRECTION
   - Rolling IQR (Q1, Q3) computed cumulatively
   - Values outside [Q1 - 1.5×IQR, Q3 + 1.5×IQR] → NaN → linear interpolate
   - Trailing IQR outliers → 3-month rolling mean
   - Warning if 5+ consecutive IQR outliers

6. FIX LAST MONTH (most recent data point)
   Three rules applied in priority order:
   - Rule 1: STL outlier AND IQR outlier → 3-month rolling average
   - Rule 2: STL outlier only → trend + seasonality + smoothed residuals
   - Rule 3: IQR outlier only → 3-month rolling average

7. FIX NEGATIVE VALUES
   Any negative or NaN values → trend + seasonality + residuals reconstruction

8. COMPUTE VALOR_CIF
   Valor_Cif = Valor × alpha  (NaN if alpha is unavailable)

9. GUARDRAIL: COMPLETE MONTHLY COVERAGE
   Ensures every month in the final date range is present
   Uses interior linear interpolation only (no extrapolation at tails)
```

### Output Schema

| Column | Description |
|---|---|
| `ID` | `IDIndicePrincipal` |
| `Data` | Month (`datetime`) |
| `Valor` | Processed FOB/weight price index |
| `Valor_Cif` | Processed CIF/weight price index (NaN if no CIF data) |

---

## 9. Folder and File Naming Conventions

```
NM/
├── serie_temporal/
│   ├── COMEX_{ISO}/                          ← one folder per country
│   │   ├── __init__.py
│   │   └── COMEX_{ISO}_NM_v2.py
│   ├── costdrivers_comex_NM_v2.py            ← base class (do not modify)
│   ├── nm_config.py                          ← NMConfig dataclass (do not modify)
│   ├── nm_filters.py                         ← filter functions (do not modify)
│   ├── nm_reasons.py                         ← DropReason enum (do not modify)
│   └── data-contract.yaml                    ← add your country's section here
│
└── dados/
    └── {ISO}/
        ├── cache/                            ← same-day download cache
        │   └── {ISO}_{date}_import.parquet
        ├── raw/                              ← per-run raw output
        │   ├── import_raw_{date}.parquet
        │   └── export_raw_{date}.parquet
        ├── database/
        │   └── historical.parquet            ← growing append-only database
        ├── silver/
        │   ├── silver_{date}.parquet         ← post-filter data
        │   └── dropped_{date}.parquet        ← dropped series audit log
        └── gold/
            └── gold_NM_v2_{date}.parquet     ← final index values
```

**Naming pattern:** `{ISO}` is the 3-letter uppercase ISO country code used consistently everywhere (folder names, class names, method calls, `iso_code` constructor parameter).

---

## 10. Testing Without Uploading

The pipeline's `run()` method accepts a `skip_phases` list to run only specific phases. This is used to iterate quickly without re-downloading or re-uploading.

```python
# Run only the calculation phase (skip collect, update_historical, normalize, upload)
pipeline = ComexXXXPipelineNMv2(developing=True)
pipeline.run(skip_phases=["collect", "update_historical", "normalize_historical", "upload"])

# Run everything except upload
pipeline.run(skip_phases=["upload"])
```

**Testing the gold output comparison (BRA pattern):**

See `serie_temporal/tests/BRA/` for examples. The pattern is:
1. Run the full pipeline in developing mode (or skip phases) to generate a gold parquet
2. Compare against a reference gold parquet or against the platform's current values

**Working directory requirement:** in `developing=True` mode, all paths are resolved relative to `cwd` using the pattern `NM/dados/{ISO}/`. You must run the pipeline from the `Comex-Derik/` directory — not the repo root. Running from the wrong directory causes silent path misses with no clear error.

```powershell
cd C:\...\GEP\Comex-Derik
python NM\serie_temporal\COMEX_XXX\COMEX_XXX_NM_v2.py
```

**Developing mode guarantees:**
- No Azure reads or writes
- No API calls (upload is blocked)
- All reads/writes go to the local `NM/dados/{ISO}/` folder structure
- Safe to run repeatedly without side effects

---

## 11. Key Open Questions for Every New Country

Before starting implementation, answer these for your specific country:

1. **What is the official data source URL?** Gov portal, statistics bureau, customs agency?
2. **What is the update frequency?** Monthly? Is there a publication lag (e.g., data for month M arrives at M+2)?
3. **What NCM codes are tracked?** The list must match what is registered in `IDS_comex.xlsx`. Confirm with the CostDrivers platform team.
4. **Does the source provide CIF data (frete + seguro)?** If not, `Valor_Cif` will be `NaN` for this country.
5. **What country-code scheme does the source use?** UN M49? ISO-2? ISO-3? Internal codes? This determines how to build the country-code mapping file.
6. **What is the country's name in `IDS_comex.xlsx`?** The `Pais_1` value must match exactly (case-sensitive).
7. **What is the date format in the source data?** `YYYYMM`, `YYYY-MM-DD`, wide-format with month columns, etc.?
8. **Does the source require authentication or rate limiting?** Add `time.sleep()` if needed to avoid being rate-limited.
9. **Are NCM codes 8 digits or a different length?** The pipeline expects 8-digit numeric NCMs. Truncate or pad as needed.
10. **How far back does the historical data go?** Filter F2 requires at least 24 months. If the source only has 18 months, the `min_months_required` config must be lowered or the launch is delayed.

---

## 12. Pain Points to Avoid in New Implementations

These are known issues in the existing codebase. Do not repeat them in new country implementations.

| Issue | What to do instead |
|---|---|
| `time.sleep(30)` + file exports inside `calculate()` (debug code left in) | Never add debug exports or sleeps to the base pipeline methods |
| SSL verification disabled globally (`ssl._create_default_https_context = ssl._create_unverified_context`) | Add `verify=False` only to the specific `requests` call that needs it, not globally |
| `print()` mixed with `self.logger.info()` | Use `self.logger` exclusively; never use `print()` |
| Fragile file glob patterns (e.g., `"*digo*Pa*s*.xlsx"`) for country-map files | Use an exact filename or a config-driven path |
| Cascading "try 8 different paths" for file resolution | Use a single canonical path based on `self.storage_base_path` and `self.iso_code` |
| Colombia 2-digit year date format ambiguity | Always validate and document the exact date format; add a format assertion in `normalize_columns()` |
| Hardcoded production credentials in upload code | Use `os.environ` and document the required env var name |
| `funcao_airflow.py` with undefined variables | Airflow task stubs must be fully self-contained; all variables must be defined within the file or injected via Airflow Variables/Connections |
| `normalize_columns` signature wrong in this guide (old version said `ie: str`) | Actual signature is `(self, df, contract: dict, import_export: int)` — `import_export` is `int`, not `str`; always end with `return super().normalize_columns(work, contract, import_export)` |
| `ImportExport` stored as string `"Import"`/`"Export"` | Must be integer `1`/`0` — `normalize_historical()` splits by `== 0` and `== 1`; strings produce silently empty DataFrames |
| Zero/null `peso` or `valor` reaching filter F4 | Drop `<= 0` rows inside `normalize_columns()` — F4 raises a hard `ValueError` before F5 can clean them |
| Filter in `normalize_columns` only checks pre-rename column names | Check both raw form (e.g., `valor_raw_usd`) and canonical form (e.g., `valor`) — `normalize_historical` calls `normalize_columns` a second time with already-canonical column names |
| OM-era country IDs (< 382958) returning 0 records at F1 | Override `load_ids_table()` to skip the NM threshold filter; see Section 5 |
| Running pipeline from GEP root in developing mode | Must run from `Comex-Derik/` — base class builds all paths relative to `cwd` |
| Missing `COSTDRIVERS_PASSWORD` env var causing import failure | `costdrivers.py` raises `ValueError` at module import time; add `os.environ.setdefault("COSTDRIVERS_PASSWORD", "...")` before any import, including in bootstrap and migration scripts |
| No way to test pipeline without triggering the scraper | Add `run_scraper: bool = True` to `__init__`, default it to `False` in `main()`; check the flag before calling the scraper |

---

## 13. Existing Country Implementations as Reference

Use these as implementation reference, in order of complexity:

### Simplest: EU countries (ITA, DEU, FRA, etc.)
**Files:** `serie_temporal/COMEX_ITA/COMEX_ITA_NM_v2.py`  
These use `iso_database='EUR'` — they skip the collect phase entirely and filter from a shared EUR historical parquet. Good reference for `normalize_columns()` and `_country_specific_treatment()`.  
**Key pattern:** REPORTER column filtering by 2-letter ISO code; `TRADE_TYPE` → `ImportExport` mapping; `PERIOD` (YYYYMM int) → `Data` datetime conversion.

### Medium: Brazil (BRA)
**Files:** `serie_temporal/COMEX_BRA/COMEX_BRA_NM_v2.py`  
Full implementation with web scraping from gov.br. Good reference for `collect_import_data()` / `collect_export_data()` with BeautifulSoup link discovery.  
**Key pattern:** Finds links matching `ncm` + `csv` + year; reads chunked CSVs; constructs `Data` from `CO_MES + CO_ANO` columns.

### Most complex: Japan (JPN)
**Files:** `serie_temporal/COMEX_JPN/COMEX_JPN_NM.py` (v1 only, no v2 yet)  
Wide-format source data (month columns) requiring a melt operation. Good reference for non-standard CSV formats.  
**Key pattern:** 22 CSV sections per year; wide-to-long transform; `Unit2 == 'KG'` conditional for weight column selection; rate-limited with `time.sleep(0.8)`.

### OM-to-NM migration with existing scraper: India (IND)
**Files:** `serie_temporal/COMEX_IND/COMEX_IND_NM_v2.py`, `bootstrap_historical.py`, `scraper/`  
Country originally tracked under OM with existing IDs below the 382958 threshold. Scraper already had 60+ months of data in Hive-partitioned parquets. Good reference for: scraper integration via `run_range()`; `load_ids_table()` override for OM-era IDs; bootstrap script pattern; `normalize_columns` handling both raw and canonical column names.  
**Key patterns:** `load_ids_table()` overridden to skip NM threshold; `valor_raw_usd = value_usd_million * 1_000_000` computed in `normalize_columns`; `frete = seguro = NaN` (source provides no CIF data); `run_scraper` flag to skip scraping during tests; bootstrap script reads all monthly parquets and calls `normalize_columns` directly to build `historical.parquet`.

---

## Appendix: API Endpoints Used

| Endpoint | Method | Purpose | Module |
|---|---|---|---|
| `/api/v1/DataScience/option/11` | GET | Fetch existing platform data (last N months) | `serie_temporal` (upload validation) |
| `/api/v1/DataScience/UpdateOption-9` | PUT | Upload computed index values (`Valor`, `Valor_Cif`) | `serie_temporal` (upload) |
| `/api/v1/InternationalTrade` | POST | Upload raw trade records (peso, fob, etc.) | `globinho` only — not used by NM v2 |

The `library.costdrivers.ApiAsync` client (at `Comex-Derik/library/costdrivers.py`) handles auth and retry for all API calls.

## Appendix: Required Environment Variables

| Variable | Used by | Description |
|---|---|---|
| `COSTDRIVERS_API_KEY` | `globinho`, and should be used by `serie_temporal` | API authentication key |
| `COSTDRIVERS_API_EMAIL` | `globinho`, and should be used by `serie_temporal` | API authentication email |
| `COSTDRIVERS_API_PASSWORD` | `globinho`, and should be used by `serie_temporal` | API authentication password |

> **Note:** `serie_temporal/costdrivers_comex_NM_v2.py` currently hardcodes credentials in the `upload()` method. Any new country implementation should use environment variables instead, matching the pattern used in `globinho`.
