# Performance Analysis — `costdrivers_comex_NM_v2.py`

**File analysed:** `Comex-Derik/NM/serie_temporal/costdrivers_comex_NM_v2.py` (~2814 lines)
**Goal:** Identify performance bottlenecks (CPU, memory, I/O) and propose concrete fixes to reduce processing time and resource usage.

---

## Executive Summary

`costdrivers_comex_NM_v2.py` is the core framework for the COMEX Nova Metodologia (NM) trade-price pipeline. It defines an abstract base class `ComexPipelineNMv2` and a set of module-level functions that country-specific subclasses inherit. The pipeline runs six sequential phases:

1. **collect** — fetches raw import/export trade data per country via an async API or loads from local cache
2. **update_historical** — appends new raw data to a long-running Parquet historical file (append-only)
3. **normalize_historical** — renames columns per a YAML data-contract
4. **filter_data** — applies four business-logic filters (NCM mapping, min history, max gap, top-N% by weight)
5. **calculate** — runs per-ID STL decomposition, IQR outlier detection, interpolation, and CIF/FOB ratio calculation
6. **upload** — pushes the gold dataset to the Cost Drivers REST API

The dominant performance problem is the **Python-loop-over-groupby** anti-pattern repeated across five separate `calculate()` sub-steps, compounded by large intermediate copies and a full Azure download in the "does-this-file-exist" check.

### Top 5 performance issues, ranked by estimated impact

| Rank | Issue | Impact |
|------|-------|--------|
| 1 | Five nested `for _, group in df.groupby(...)` loops over every ID, each rebuilding the full DataFrame via `pd.concat` | **Very High** — O(IDs) Python iterations × 5 passes, each with a `group.copy()` |
| 2 | Row-by-row Python `for i in range(len(...))` inside `outlier_testing_iqr` (cumulative IQR) and `fill_tail_nan` | **High** — O(n) pure-Python per series, called inside groupby loop |
| 3 | `_file_exists_in_storage` fully downloads the file from ADLS just to check existence | **High** — unnecessary full I/O round-trip before every save |
| 4 | `load_data_contract()` and `load_ids_table()` re-read disk/Excel on every invocation with no instance-level memoisation | **Medium** — disk I/O repeated 3–4 times per run |
| 5 | `_apply_top_n_percent` wraps `top_n_percent` (the old v1 function) and re-does a second `groupby + cumsum` audit scan on the same data that was just aggregated | **Medium** — duplicated groupby work on the full historical dataset |

---

## Deep Dive — Performance Issues

### Issue 1 — Five separate `for _, group in df.groupby('ID')` loops in `calculate()`

**Location:** `costdrivers_comex_NM_v2.py:2072-2074`, `2086-2088`, `2107-2109`, `2124-2126`, `2049-2051`

Each sub-step follows this pattern:

```python
_results = []
for _, group in series_fob.groupby('IDIndicePrincipal'):
    _results.append(preprocess_data(group.copy(), ...))
series_preprocessed = pd.concat(_results, ignore_index=True)
```

The same pattern appears at lines 2072, 2086, 2107, 2124, and 2049. That is **five full passes** over the entire dataset — five `pd.concat` rebuilds of the full DataFrame — where each intermediate result (`series_preprocessed`, `series_clean`, `series_final`, `series_final3`) is a complete copy held in memory simultaneously with its predecessor.

**Why it is slow:**
- Every `group.copy()` allocates a new DataFrame for each ID on each pass.
- `pd.concat` of thousands of small DataFrames is O(n·k) where n is total rows and k is number of IDs — the list-of-frames pattern is known to produce quadratic memory churn in pandas.
- Five sequential passes mean peak memory is approximately **5× the size of the working dataset**.

**Recommendation:** Consolidate all per-ID transformations into a single `groupby().apply()` pipeline, or use a single `for _, group` loop that calls all five functions in sequence and accumulates one result. This eliminates four of the five `pd.concat` rebuilds and reduces peak memory by roughly 4×.

**Expected speedup: 3–8× on large country datasets (hundreds of IDs × 60+ months).**

---

### Issue 2 — O(n) pure-Python row-by-row loops in `outlier_testing_iqr` and `fill_tail_nan`

**Locations:**
- `costdrivers_comex_NM_v2.py:491-506` (`outlier_testing_iqr` — cumulative IQR loop)
- `costdrivers_comex_NM_v2.py:157-168` (`fill_tail_nan` — tail-NaN loop)
- `costdrivers_comex_NM_v2.py:447-458` (`outlier_testing_stl` — smoothed residuals loop)

`outlier_testing_iqr` lines 491–506:
```python
for i in range(len(raw_values_np)):
    past_raw = raw_values_np[:i+1]
    past_raw = past_raw[~np.isnan(past_raw)]
    q1 = np.percentile(past_raw, 25)
    ...
```

This is an **O(n²) pattern**: each iteration slices a growing prefix of the array and calls `np.percentile` on it. For a 60-month series it is 60 percentile computations; for a 120-month series, 120. Multiplied by hundreds of IDs this becomes the slowest part of the per-series math.

`fill_tail_nan` lines 157–168 is a simpler O(n) loop but still calls `values.iloc[i]` (label-based pandas indexer) inside the loop, which is significantly slower than numpy array access.

`outlier_testing_stl` lines 447–458 loops over every row to smooth residuals using `residuals.iloc[:idx]` — a growing prefix slice per iteration.

**Recommendation:**
- `outlier_testing_iqr`: Replace the cumulative-percentile loop with `pd.expanding().quantile(0.25/0.75)`. This is a single vectorised pandas call. **Expected speedup: 20–50× per series.**
- `fill_tail_nan`: Convert `values` to a numpy array, operate with numpy indexing, write back once. **5–10× speedup.**
- `outlier_testing_stl` residual smoothing: Use `pd.Series.where(...).rolling(3).mean()` with `min_periods=1` applied only to outlier positions. **10–20× speedup per series.**

---

### Issue 3 — `_file_exists_in_storage` fully downloads the blob just to check existence

**Location:** `costdrivers_comex_NM_v2.py:2716-2719`

```python
# Tentar baixar o arquivo para verificar se existe
# Se não existir, download_adls2 lançará exceção
self.azure_storage.download_adls2(azure_path)
return True
```

The comment is self-explanatory: the check works by performing a full download. This is called from `_save_to_storage` (line 2541) **before every write**. For large Parquet files (historical.parquet can be hundreds of MB) this doubles the I/O cost of every save operation.

**Recommendation:** Replace with a metadata/HEAD call. If `AzureBlobStorage` exposes a `get_file_properties` or equivalent, use that. If not, wrap the Azure SDK's `DataLakeFileClient.get_file_properties()` directly. If neither is feasible quickly, remove the existence check entirely — parquet saves are idempotent and the check-then-overwrite logic provides no correctness guarantee anyway (TOCTOU).

**Expected impact: eliminates one full blob download per save call, which can be 1–5 seconds per file on a slow network.**

---

### Issue 4 — `load_data_contract()` and `load_ids_table()` re-read disk on every call, with no caching

**Locations:**
- `load_data_contract` called at lines **1515, 1619, 1775** — three times per run in the worst case
- `load_ids_table` called at line **1909** — once per run, but `pd.read_excel` on an XLSX file is expensive (openpyxl is slow; a 5 000-row IDS_comex.xlsx can take 2–4 s)
- `load_data_contract` opens a YAML file and iterates through up to 5 candidate paths on disk each call (lines 1133–1143)

**Recommendation:**
- Add `@functools.lru_cache(maxsize=None)` or simple instance-level `_contract_cache` / `_ids_cache` attributes. Set them on first load and return the cached value on subsequent calls. One-line change per method.

**Saves 2–8 s per run for the Excel load.**

---

### Issue 5 — `_apply_top_n_percent` duplicates work already done inside `top_n_percent`

**Location:** `costdrivers_comex_NM_v2.py:1958-1989`

`_apply_top_n_percent` (line 1959) calls `top_n_percent(df.copy(), threshold)` (line 1968), which internally runs two `groupby` aggregations, a sort, three `cumsum` calls, and a merge. Then `_apply_top_n_percent` immediately runs a **third** `groupby` (line 1967) on the pre-aggregated data to count countries for the audit log, and a fourth merge (line 1975).

`top_n_percent` itself (lines 316–337) also performs three separate `por_pais.groupby(grupo_cols)[col].cumsum()` calls one after another rather than combining them in a single `groupby.agg`. Each cumsum triggers a separate groupby pass.

**Recommendation:**
- Collapse the three `cumsum` calls at lines 316–318 into one `groupby(grupo_cols).cumsum()` applied to a multi-column selection.
- Have `top_n_percent` (or `filter_top_percent` in `nm_filters.py`, which is the cleaner v2 implementation) return the per-ID country counts as a side-channel, so `_apply_top_n_percent` does not need to re-scan.
- Note that `nm_filters.filter_top_percent` (in `nm_filters.py`) already does this correctly and more efficiently. Consider replacing the call to the legacy `top_n_percent` at line 1968 with `filter_top_percent`.

**Expected speedup: modest (10–20%) but removes code duplication.**

---

## Quick Wins vs. Bigger Refactors

### Quick wins (< 1 hour each)

#### QW-1 — Cache `load_data_contract` and `load_ids_table`
**Location:** `costdrivers_comex_NM_v2.py:1106`, `1149`

Add two instance attributes (`_contract_cache`, `_ids_cache`) set to `None` in `__init__`, check them at the top of each method, and return early if populated. **Zero risk**, eliminates repeated disk I/O.

#### QW-2 — Collapse the five groupby loops in `calculate()` into one
**Location:** `costdrivers_comex_NM_v2.py:2049-2128`

Change the five separate loops to a single loop that calls all five functions in sequence per group, accumulating into one result list. This is a direct mechanical change — no algorithm change needed — and cuts peak memory by ~4×.

#### QW-3 — Remove the redundant full-download in `_file_exists_in_storage`
**Location:** `costdrivers_comex_NM_v2.py:2716-2719`

Either use a metadata API call or unconditionally overwrite (parquet saves are idempotent). One-line fix.

#### QW-4 — Replace `outlier_testing_iqr` cumulative-percentile loop with `pd.expanding().quantile()`
**Location:** `costdrivers_comex_NM_v2.py:491-506`

```python
# Replace lines 491-506 with:
s = pd.Series(raw_values_np)
series_clean['cumulative_Q1'] = s.expanding().quantile(0.25).values
series_clean['cumulative_Q3'] = s.expanding().quantile(0.75).values
series_clean['cumulative_IQR'] = series_clean['cumulative_Q3'] - series_clean['cumulative_Q1']
```

**20–50× speedup per series.** This is the highest per-line ROI change in the file.

#### QW-5 — Collapse the three separate `cumsum` calls in `top_n_percent`
**Location:** `costdrivers_comex_NM_v2.py:316-318`

```python
# Replace lines 316-318 with:
cum_cols = por_pais.groupby(grupo_cols)[['cif_by_country','fob_by_country','kg_by_country']].cumsum()
por_pais[['cum_cif_by_country','cum_fob_by_country','cum_kg_by_country']] = cum_cols
```

One groupby pass instead of three.

---

### Bigger refactors (hours to days, higher reward)

#### BR-1 — Vectorise `fill_tail_nan` and the STL residual-smoothing loop
**Location:** `costdrivers_comex_NM_v2.py:140-176`, `442-463`

These require replacing tail-detection logic and rolling-backward mean with numpy operations. The logic is non-trivial (finding the last non-NaN run) but can be vectorised with `np.where` and `pd.Series.shift` chains.

#### BR-2 — Replace the legacy `top_n_percent` call with `nm_filters.filter_top_percent`

`filter_top_percent` in `nm_filters.py` already eliminates the `acumular_codigos_pais` inner Python loop (lines 341–350 of the main file), uses `shift` for the threshold comparison, and returns the audit drops natively. Retiring `top_n_percent` also removes ~170 lines of dead code.

#### BR-3 — Use `concat` pre-allocation instead of list-append-then-concat

Several loops (`upload:2409-2419`, `calculate:2071-2074`) append to a Python list and then call `pd.concat`. This is fine for small lists but for large numbers of IDs it degrades. The real fix here is BR-1: once the groupby loops are collapsed (QW-2), the concat occurs only once and at the right granularity.

#### BR-4 — Move `import re` inside `_load_from_storage` to top-level imports
**Location:** `costdrivers_comex_NM_v2.py:2608`

`import re` is called inside the method body on every invocation. Move it to the top-level imports at line 29. Minor but zero-cost correctness fix.

---

## Additional Context & Observations

- `nm_filters.py` contains a parallel, cleaner implementation of the top-percent filter (`filter_top_percent`) that supersedes `top_n_percent` in the main file but is only used directly in `filter_data`'s comment text — `filter_data` still calls `_apply_top_n_percent` → `top_n_percent`.
- The `acumular_codigos_pais` nested function at `costdrivers_comex_NM_v2.py:342-350` is defined but never actually called (the code below it uses a different path via `por_pais_contrib.groupby(...).apply(lambda x: str(x.tolist()))`). **It is dead code.**
- Commented-out `df.drop(columns=...)` calls at lines **433, 464, 473, 511, 679, 713** suggest diagnostic columns (`residuals_mean`, `residuals_std`, `residuals_zscore`, `trend`, `seasonality`, etc.) are being carried forward into every intermediate DataFrame. **Re-enabling these drops would reduce per-frame memory by 30–50% for the STL-heavy steps.**

---

## Suggested Implementation Order

1. **QW-1** (cache loaders) — trivial, zero risk
2. **QW-4** (expanding quantile in `outlier_testing_iqr`) — highest per-line ROI
3. **QW-3** (remove existence-check download)
4. **QW-5** (single cumsum in `top_n_percent`)
5. **QW-2** (collapse five groupby loops in `calculate`) — biggest memory win
6. Re-enable commented-out `df.drop` calls (lines 433, 464, 473, 511, 679, 713)
7. **BR-4** (move `import re` to top)
8. **BR-1** (vectorise `fill_tail_nan` + STL smoothing)
9. **BR-2** (retire legacy `top_n_percent`, switch to `filter_top_percent`)
10. **BR-3** (concat strategy review — only after QW-2)

---

## Estimated Cumulative Impact

| Bucket | Wall-clock | Peak memory |
|--------|-----------|-------------|
| Quick wins only | **2–5× faster** | **~3× lower** |
| Quick wins + Bigger refactors | **5–15× faster** | **~5× lower** |

Numbers assume a country dataset of several hundred IDs × 60+ months. Smaller datasets see less benefit; larger ones (BRA, IND) see more.
