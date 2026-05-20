# Comex NM v2 — Code Patterns Reference

**Purpose:** Concrete, copy-paste-ready code patterns for new country implementations.
This file is the companion to `COMEX_NM_V2_GUIDE.md` — the guide explains *why*,
this file shows *exactly what to write*.

**Existing implementations:** BRA (web scraping), ITA (shared EUR historical), IND (parquet scraper + OM-era IDs).

---

## Decision Tree: Which Pattern Applies?

```
Does the country share a historical database with other countries (e.g., EUR)?
│
├─ YES → Pattern A (Shared Historical)   e.g. ITA, DEU, FRA
│
└─ NO → Does the source provide a pre-built parquet dataset (own scraper)?
         │
         ├─ YES → Pattern C (Parquet Scraper)   e.g. IND
         │         └─ Are the existing IDs < 382958 (OM-era)?
         │              ├─ YES → also add Override: load_ids_table
         │              └─ NO  → standard IDs, no override needed
         │
         └─ NO → Pattern B (Web Scraping)   e.g. BRA
```

---

## Pattern A — Shared Historical (EUR-style)

Use when the country's data comes from a shared source already loaded into a
shared `historical.parquet` (e.g., EUR Comext data covering all EU countries).

```python
class COMEX_ITA_NM_v2(ComexPipelineNMv2):

    def __init__(self, config=None, start_date=None, use_azure=True, developing=False):
        super().__init__(
            iso_code="ITA",           # 3-letter ISO code
            iso_database="EUR",       # shared historical — collect is skipped automatically
            config=config,
            start_date=start_date,
            data_contract_path="data-contract.yaml",
            ids_table_path="IDS_comex.xlsx",
            use_azure=use_azure,
            developing=developing,
        )

    def _get_country_name(self) -> str:
        return "Itália"               # must match Pais_1 in IDS_comex.xlsx exactly

    def _country_specific_treatment(self, df: pd.DataFrame) -> pd.DataFrame:
        # All column prep happens here — normalize_columns is NOT overridden.
        # At this point columns still have raw EUR Comext names.

        if "PRODUCT_NC" in df.columns:
            df = df[df["PRODUCT_NC"] != "TOTAL"].copy()
            df = df[~df["PRODUCT_NC"].isnull()].copy()

        # Date from PERIOD (YYYYMM int)
        if "PERIOD" in df.columns:
            df["Data"] = pd.to_datetime(df["PERIOD"].astype(str), format="%Y%m", errors="coerce")
            df = df.drop(columns=["PERIOD"])

        # NCM cleanup
        if "PRODUCT_NC" in df.columns:
            df["PRODUCT_NC"] = pd.to_numeric(df["PRODUCT_NC"], errors="coerce").astype("Int64")
            df = df.dropna(subset=["PRODUCT_NC"]).astype({"PRODUCT_NC": "int32"})

        # No CIF data from EUR source
        df["frete"] = np.nan
        df["seguro"] = np.nan

        # Rename TRADE_TYPE → ImportExport (data contract will handle the rest)
        if "TRADE_TYPE" in df.columns:
            df = df.rename(columns={"TRADE_TYPE": "ImportExport"})

        # Filter out reporter column (country-level, not needed)
        if "REPORTER" in df.columns:
            df = df.drop(columns=["REPORTER"])

        return df
```

**Key points:**
- `iso_database="EUR"` makes the base class skip `collect_import_data` / `collect_export_data`
- `collect_*` methods do NOT need to be implemented
- All column work goes in `_country_specific_treatment` (runs before `normalize_columns`)
- `normalize_columns` is NOT overridden — the data contract YAML handles the rename

---

## Pattern B — Web Scraping

Use when the country has no pre-built parquet dataset and requires live HTTP scraping.

```python
class COMEX_BRA_NM_v2(ComexPipelineNMv2):

    BASE_URL = "https://..."   # official source URL

    def __init__(self, config=None, start_date=None, use_azure=True, developing=False):
        super().__init__(
            iso_code="BRA",
            config=config,
            start_date=start_date,
            data_contract_path="data-contract.yaml",
            ids_table_path="IDS_comex.xlsx",
            use_azure=use_azure,
            developing=developing,
        )

    def _get_country_name(self) -> str:
        return "Brasil"

    def _process_ie_type(self, ie_type: str) -> pd.DataFrame:
        # Core scraping logic shared between import and export.
        # Set ImportExport as INTEGER here — not in normalize_columns.
        files = self._get_available_files(ie_type)
        dfs = []
        for info in files:
            df = self._download_and_parse(info)
            if df is None or df.empty:
                continue
            df["Data"] = pd.to_datetime(...)          # build date from source columns
            df["ImportExport"] = 1 if ie_type == "import" else 0   # INTEGER — not string
            dfs.append(df)
        return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

    def collect_import_data(self) -> pd.DataFrame:
        return self._process_ie_type("import")

    def collect_export_data(self) -> pd.DataFrame:
        return self._process_ie_type("export")

    def _country_specific_treatment(self, df: pd.DataFrame) -> pd.DataFrame:
        # Drop source-only columns not needed downstream
        for col in ("CO_MES", "CO_ANO"):
            if col in df.columns:
                df = df.drop(columns=col)
        return df
```

**Key points:**
- `ImportExport` must be integer `1`/`0` — set it where the raw data is processed, not in normalize_columns
- `normalize_columns` is NOT overridden — the data contract YAML handles column rename
- Do NOT disable SSL globally (`ssl._create_default_https_context = ...`); use `verify=False` per request
- Add `time.sleep()` between requests if the source rate-limits

---

## Pattern C — Parquet Scraper

Use when the country has a pre-built scraper that writes Hive-partitioned parquets
(`year=YYYY/month=MM/*.parquet`) and the pipeline reads from them instead of scraping live.

```python
class COMEX_IND_NM_v2(ComexPipelineNMv2):

    def __init__(
        self,
        config=None,
        start_date=None,
        use_azure=True,
        developing=False,
        run_scraper=True,    # set False during testing to skip the scraper call
    ):
        super().__init__(
            iso_code="IND",
            config=config,
            start_date=start_date,
            data_contract_path="data-contract.yaml",
            ids_table_path="IDS_comex.xlsx",
            use_azure=use_azure,
            developing=developing,
        )
        self.run_scraper = run_scraper

    def _get_country_name(self) -> str:
        return "Índia"

    # --- Parquet reading ---

    def _scraper_parquet_dir(self) -> Path:
        # Resolve the base directory where scraped parquets live.
        candidates = [
            _PROJECT_ROOT / "NM" / "dados" / self.iso_code / "scraper_parquet",
        ]
        for cand in candidates:
            if cand.exists():
                return cand
        fallback = candidates[0]
        fallback.mkdir(parents=True, exist_ok=True)
        return fallback

    def _list_scraped_months(self, base_dir: Path):
        months = []
        for year_dir in sorted(base_dir.glob("year=*")):
            try:
                year = int(year_dir.name.split("=", 1)[1])
            except (ValueError, IndexError):
                continue
            for month_dir in sorted(year_dir.glob("month=*")):
                try:
                    month = int(month_dir.name.split("=", 1)[1])
                except (ValueError, IndexError):
                    continue
                if any(month_dir.glob("*.parquet")):
                    months.append((year, month))
        return sorted(set(months))

    def _maybe_scrape_next_month(self, base_dir: Path) -> None:
        if not self.run_scraper:
            self.logger.info("run_scraper=False — pulando scrape.")
            return
        # ... check if newest scraped month is stale, call orchestrator.run_range() if so

    def _read_scraped_dataset(self, base_dir: Path, flow: str) -> pd.DataFrame:
        parquet_files = []
        for year, month in self._list_scraped_months(base_dir):
            if (year, month) < (self.start_date.year, self.start_date.month):
                continue
            month_dir = base_dir / f"year={year}" / f"month={month:02d}"
            parquet_files.extend(str(p) for p in sorted(month_dir.glob("*.parquet")))
        if not parquet_files:
            return pd.DataFrame()
        dataset = ds.dataset(parquet_files, format="parquet")
        return dataset.to_table(filter=ds.field("trade_flow") == flow).to_pandas()

    def _collect_trade(self, flow: str) -> pd.DataFrame:
        base_dir = self._scraper_parquet_dir()
        self._maybe_scrape_next_month(base_dir)
        return self._read_scraped_dataset(base_dir, flow)

    def collect_import_data(self) -> pd.DataFrame:
        return self._collect_trade("IMPORT")

    def collect_export_data(self) -> pd.DataFrame:
        return self._collect_trade("EXPORT")

    # --- normalize_columns override (required when source has unit conversions) ---

    def normalize_columns(self, df: pd.DataFrame, contract: dict, import_export: int) -> pd.DataFrame:
        if df is None or df.empty:
            return df
        work = df.copy()

        # Compute intermediate columns — only when raw names are present (first call).
        if "value_usd_million" in work.columns:
            work["valor_raw_usd"] = (
                pd.to_numeric(work["value_usd_million"], errors="coerce").astype("float64")
                * 1_000_000.0
            )

        # Build Data from year + month columns
        if "year" in work.columns and "month" in work.columns:
            year_int = pd.to_numeric(work["year"], errors="coerce").astype("Int64")
            month_int = pd.to_numeric(work["month"], errors="coerce").astype("Int64")
            yyyymmdd = year_int.astype("int64") * 10000 + month_int.astype("int64") * 100 + 1
            work["Data"] = pd.to_datetime(yyyymmdd.astype(str), format="%Y%m%d", errors="coerce")

        # Map trade_flow string → ImportExport integer
        if "trade_flow" in work.columns:
            work["ImportExport"] = work["trade_flow"].astype(str).map({"IMPORT": 1, "EXPORT": 0})

        # No CIF data from this source
        work["frete"] = np.nan
        work["seguro"] = np.nan

        # Drop zero/null peso and valor EARLY — F4 (top_n_percent) crashes before F5 can clean.
        # Check both raw name (first call) and canonical name (second call from normalize_historical).
        valor_col = next((c for c in ("valor_raw_usd", "valor") if c in work.columns), None)
        if valor_col:
            mask = pd.to_numeric(work[valor_col], errors="coerce").fillna(0) > 0
            dropped = int((~mask).sum())
            if dropped:
                self.logger.info("normalize_columns: %d rows dropped — %s null/zero", dropped, valor_col)
            work = work.loc[mask].copy()

        peso_col = next((c for c in ("quantity", "peso") if c in work.columns), None)
        if peso_col:
            mask = pd.to_numeric(work[peso_col], errors="coerce").fillna(0) > 0
            dropped = int((~mask).sum())
            if dropped:
                self.logger.info("normalize_columns: %d rows dropped — %s null/zero", dropped, peso_col)
            work = work.loc[mask].copy()

        return super().normalize_columns(work, contract, import_export)

    def _country_specific_treatment(self, df: pd.DataFrame) -> pd.DataFrame:
        if df is None or df.empty:
            return df
        result = df.copy()
        if "ncm" in result.columns:
            ncm_num = pd.to_numeric(result["ncm"], errors="coerce")
            result = result.loc[ncm_num.notna()].copy()
            result["ncm"] = ncm_num.loc[result.index].astype("int64")
        if "pais_id" in result.columns:
            pid_num = pd.to_numeric(result["pais_id"], errors="coerce")
            result = result.loc[pid_num.notna()].copy()
            result["pais_id"] = pid_num.loc[result.index].astype("int64")
        return result
```

**Key points:**
- `run_scraper=True` in `__init__`, default `False` in `main()` — never triggers scraper during tests
- `normalize_columns` is overridden because the source requires a unit conversion (`* 1_000_000`)
- Dual filtering (raw name → canonical name fallback) is mandatory — `normalize_historical` calls `normalize_columns` a second time with already-canonical column names
- Always call `return super().normalize_columns(work, contract, import_export)` at the end

---

## Override: `load_ids_table` for OM-Era IDs

Apply this override when the country's existing `IDIndicePrincipal` values are all
below 382958 (registered before NM was introduced). Without it, `load_ids_table`
filters them all out and F1 drops everything silently.

**Symptom:** log shows `IDs carregados: 0 registros para {country}`.

```python
def load_ids_table(self) -> pd.DataFrame:
    # This country uses OM-era IDs (all < 382958) — skip the NM threshold filter.
    candidates = [
        Path("library/IDS_comex.xlsx"),
        _PROJECT_ROOT / "Comex-Derik" / "library" / "IDS_comex.xlsx",
        _PROJECT_ROOT / "library" / "IDS_comex.xlsx",
    ]
    ids = None
    for path in candidates:
        if path.exists():
            ids = pd.read_excel(path)
            self.logger.info("IDS table loaded from: %s", path.absolute())
            break
    if ids is None:
        raise FileNotFoundError("IDS_comex.xlsx not found in any candidate path")

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

---

## Common: `__init__` Boilerplate

```python
def __init__(
    self,
    config: Optional[NMConfig] = None,
    start_date: Optional[datetime] = None,
    use_azure: bool = True,
    developing: bool = False,
) -> None:
    super().__init__(
        iso_code="XXX",                          # 3-letter ISO
        config=config,
        start_date=start_date,
        data_contract_path="data-contract.yaml",
        ids_table_path="IDS_comex.xlsx",
        use_azure=use_azure,
        developing=developing,
        # iso_database="EUR",                   # uncomment for shared historical countries
    )
```

---

## Common: `main()` Boilerplate

```python
def main(do_upload: bool = False, run_scraper: bool = False):
    today = datetime.now()
    start = (today.replace(day=1) - pd.DateOffset(months=5)).to_pydatetime()

    pipeline = COMEX_XXX_NM_v2(start_date=start, developing=True, run_scraper=run_scraper)
    skip = [] if do_upload else ["upload"]
    pipeline.run(skip_phases=skip)
    return pipeline


if __name__ == "__main__":
    import sys as _sys
    do_upload = "--with-upload" in _sys.argv
    run_scraper = "--run-scraper" in _sys.argv
    p = main(do_upload=do_upload, run_scraper=run_scraper)
    n_silver = len(p.silver_df) if getattr(p, "silver_df", None) is not None else 0
    n_dropped = len(p.dropped_df) if getattr(p, "dropped_df", None) is not None else 0
    n_gold = len(p.gold_df) if getattr(p, "gold_df", None) is not None else 0
    print(
        f"\nPipeline XXX v2 concluído.\n"
        f"  silver:    {n_silver} linhas\n"
        f"  dropped:   {n_dropped} linhas\n"
        f"  gold v2:   {n_gold} linhas\n"
        f"  upload:    {'SIM' if do_upload else 'PULADO (use --with-upload para subir)'}"
    )
```

---

## Data-Contract YAML Template

```yaml
XXX:
  export:
    raw_ncm_col: ncm               # raw scraper column → canonical name
    raw_country_id_col: pais_id
    raw_country_name_col: pais_name
    raw_weight_col: peso
    valor_raw_usd: valor           # intermediate computed in normalize_columns()
    columns: ['Data', 'ncm', 'ImportExport', 'pais_id', 'pais_name', 'peso', 'valor', 'frete', 'seguro']
  import:
    raw_ncm_col: ncm
    raw_country_id_col: pais_id
    raw_country_name_col: pais_name
    raw_weight_col: peso
    valor_raw_usd: valor
    columns: ['Data', 'ncm', 'ImportExport', 'pais_id', 'pais_name', 'peso', 'valor', 'frete', 'seguro']
```

Keys are raw column names; values are canonical names. The `columns` list defines
which columns survive after renaming — order does not matter. Keys missing from
the DataFrame are silently skipped (rename is non-strict).

For Pattern A (EUR shared historical), the raw column names come from the EUR
Comext format (`PRODUCT_NC`, `VALUE_IN_EUROS`, `QUANTITY_IN_KG`, etc.).

---

## Bootstrap Script Template (Pattern C only)

When the scraper already has months of data before the pipeline runs for the first time,
write a `bootstrap_historical.py` to populate `historical.parquet`:

```python
"""One-shot bootstrap: reads all scraped monthly parquets, normalises, writes historical.parquet."""
import os, sys
os.environ.setdefault("COSTDRIVERS_PASSWORD", "bootstrap-not-used")   # must be before imports
os.environ.setdefault("COSTDRIVERS_ENDPOINT", "https://api-costdrivers.gep.com/costdrivers-api")

from pathlib import Path
import pandas as pd, yaml
from COMEX_XXX.COMEX_XXX_NM_v2 import COMEX_XXX_NM_v2

SCRAPER_PARQUET_DIR = Path("NM/dados/XXX/scraper_parquet")
HISTORICAL_OUT = Path("NM/dados/XXX/database/historical.parquet")
DATA_CONTRACT_PATH = Path("NM/serie_temporal/data-contract.yaml")

pipeline = COMEX_XXX_NM_v2(start_date=datetime(2000, 1, 1), developing=True, use_azure=False)
contract = yaml.safe_load(DATA_CONTRACT_PATH.read_text(encoding="utf-8"))

parts = []
for flow_label, import_export in (("IMPORT", 1), ("EXPORT", 0)):
    frames = []
    for year_dir in sorted(SCRAPER_PARQUET_DIR.glob("year=*")):
        for month_dir in sorted(year_dir.glob("month=*")):
            for pq_file in sorted(month_dir.glob("*.parquet")):
                df = pd.read_parquet(pq_file)
                df_flow = df[df["trade_flow"].astype(str) == flow_label]
                if not df_flow.empty:
                    frames.append(df_flow.copy())
    if not frames:
        continue
    raw = pd.concat(frames, ignore_index=True)
    normalized = pipeline.normalize_columns(raw, contract, import_export)
    normalized = pipeline._country_specific_treatment(normalized)
    parts.append(normalized)

historical = pd.concat(parts, ignore_index=True)
HISTORICAL_OUT.parent.mkdir(parents=True, exist_ok=True)
historical.to_parquet(HISTORICAL_OUT, engine="pyarrow", compression="snappy", index=False)
print(f"Written {len(historical)} rows to {HISTORICAL_OUT}")
```

**Warning:** this script writes canonical column names. When the pipeline later
runs `normalize_historical()`, it calls `normalize_columns()` a second time with
those canonical names. Your filters must handle both forms — see Pattern C.

---

## Pre-Flight Checklist

Before running the pipeline for the first time:

```
[ ] _get_country_name() return value matches Pais_1 in IDS_comex.xlsx exactly (case-sensitive)
[ ] IDS_comex.xlsx has entries for this country
[ ] ImportExport is integer (1/0), not string — confirmed in collect or normalize_columns
[ ] data-contract.yaml has an XXX section with correct raw column names
[ ] historical.parquet exists with >= 24 months (or min_months_required is lowered in NMConfig)
[ ] Running from Comex-Derik/ directory (not GEP root) in developing mode
[ ] COSTDRIVERS_PASSWORD set before any import in pipeline file and bootstrap script
[ ] Zero/null peso and valor filtered in normalize_columns (both raw and canonical names)
[ ] If IDs < 382958: load_ids_table() overridden to skip NM threshold
[ ] run_scraper=False used during initial testing
```
