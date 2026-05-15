import logging
import sys
from concurrent.futures import FIRST_COMPLETED, Future, ProcessPoolExecutor, wait
from datetime import datetime
from logging.handlers import RotatingFileHandler
from pathlib import Path

import pandas as pd

from .checkpoint import is_month_complete, write_checkpoint_json, write_parquet
from .constants import (
    END_MONTH,
    END_YEAR,
    EXPECTED_COLUMNS,
    LOGS_DIR,
    MIN_LIVE_PROXIES,
    MONTH_NAMES,
    NUM_WORKERS,
    PROXIES_CSV,
    REPORT_VAL_QTY,
    REPORT_VAL_USD,
    START_MONTH,
    START_YEAR,
    IMPORT_URL,
    LOG_FORMAT_MAIN,
)
from .month_worker import run_worker
from .proxy_manager import load_and_test_proxies

log = logging.getLogger(__name__)

TASKS: list[tuple[str, int, int]] = [
    ("IMPORT", REPORT_VAL_USD, 0),
    ("IMPORT", REPORT_VAL_QTY, 1),
    ("EXPORT", REPORT_VAL_USD, 2),
    ("EXPORT", REPORT_VAL_QTY, 3),
]


def setup_logging(log_dir: Path) -> tuple[logging.Logger, Path]:
    log_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = log_dir / f"scraper_{timestamp}.log"

    root = logging.getLogger()
    root.setLevel(logging.INFO)
    for h in list(root.handlers):
        root.removeHandler(h)

    fmt = logging.Formatter(LOG_FORMAT_MAIN, datefmt="%Y-%m-%d %H:%M:%S")

    fh = RotatingFileHandler(log_file, maxBytes=50 * 1024 * 1024, backupCount=5, encoding="utf-8")
    fh.setFormatter(fmt)
    root.addHandler(fh)

    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(fmt)
    root.addHandler(ch)

    return root, log_file


def get_live_proxies(proxies_csv: Path) -> list[dict]:
    log.info("Testing proxies from %s ...", proxies_csv)
    live = load_and_test_proxies(proxies_csv, IMPORT_URL, timeout=10)
    log.info("Live proxies: %d / available in CSV", len(live))
    if len(live) < MIN_LIVE_PROXIES:
        raise RuntimeError(f"Only {len(live)} live proxies — need at least {MIN_LIVE_PROXIES}")
    return live


def generate_months(
    start_year: int, start_month: int, end_year: int, end_month: int
) -> list[tuple[int, int]]:
    months = []
    y, m = start_year, start_month
    while (y, m) <= (end_year, end_month):
        months.append((y, m))
        m += 1
        if m > 12:
            m = 1
            y += 1
    return months


def merge_worker_results(
    results: list[tuple[pd.DataFrame, list[int]]],
    year: int,
    month: int,
) -> tuple[pd.DataFrame, list[int]]:
    (df_imp_usd, f0), (df_imp_qty, f1), (df_exp_usd, f2), (df_exp_qty, f3) = results

    def _prep(df: pd.DataFrame, value_col: str, keep_unit: bool = False) -> pd.DataFrame:
        base_cols = ["country_id", "country_name", "hs_code", "hs_description", value_col]
        if df.empty:
            return pd.DataFrame(columns=base_cols + (["unit"] if keep_unit else []))
        out = df.copy()
        out["hs_code"] = out["hs_code"].astype(str).str.zfill(8)
        out = out.rename(columns={"raw_value": value_col})
        select = base_cols + (["unit"] if keep_unit and "unit" in out.columns else [])
        return out[select]

    imp_usd = _prep(df_imp_usd, "value_usd_million")
    imp_qty = _prep(df_imp_qty, "quantity", keep_unit=True)
    exp_usd = _prep(df_exp_usd, "value_usd_million")
    exp_qty = _prep(df_exp_qty, "quantity", keep_unit=True)

    def _join(df_usd: pd.DataFrame, df_qty: pd.DataFrame, trade_flow: str) -> pd.DataFrame:
        merged = pd.merge(df_usd, df_qty, on=["country_id", "hs_code"], how="outer", suffixes=("_u", "_q"))
        merged["country_name"] = merged["country_name_u"].fillna(merged.get("country_name_q", pd.Series(dtype=str)))
        merged["hs_description"] = merged["hs_description_u"].fillna(merged.get("hs_description_q", pd.Series(dtype=str)))
        merged["quantity_unit"] = merged["unit"] if "unit" in merged.columns else None
        drop = ["country_name_u", "country_name_q", "hs_description_u", "hs_description_q", "unit"]
        merged = merged.drop(columns=[c for c in drop if c in merged.columns])
        merged["trade_flow"] = trade_flow
        return merged

    df_import = _join(imp_usd, imp_qty, "IMPORT")
    df_export = _join(exp_usd, exp_qty, "EXPORT")

    non_empty = [d for d in [df_import, df_export] if not d.empty]
    df = pd.concat(non_empty, ignore_index=True) if non_empty else pd.DataFrame(columns=df_import.columns)
    df["year"] = year
    df["month"] = month

    df["year"] = df["year"].astype("int16")
    df["month"] = df["month"].astype("int8")
    df["country_id"] = df["country_id"].astype("int16")
    df["country_name"] = df["country_name"].astype("string")
    df["hs_code"] = df["hs_code"].astype("string")
    df["hs_description"] = df["hs_description"].astype("string")
    df["value_usd_million"] = df["value_usd_million"].astype("float32")
    df["quantity"] = df["quantity"].astype("float32")
    df["quantity_unit"] = df["quantity_unit"].astype("string")
    df["trade_flow"] = df["trade_flow"].astype(pd.CategoricalDtype(categories=["IMPORT", "EXPORT"]))

    df = df[EXPECTED_COLUMNS]

    all_failed = sorted(set(f0) | set(f1) | set(f2) | set(f3))
    return df, all_failed


def run_month(
    year: int,
    month: int,
    live_proxies: list[dict],
    executor: ProcessPoolExecutor,
    log_file: Path,
) -> bool:
    if len(live_proxies) < NUM_WORKERS:
        log.error("FAILED %04d-%02d: not enough live proxies (%d<%d)", year, month, len(live_proxies), NUM_WORKERS)
        return False

    active: dict[int, dict] = {wid: live_proxies[i] for i, (_, _, wid) in enumerate(TASKS)}
    spare_pool: list[dict] = list(live_proxies[NUM_WORKERS:])

    pending: dict[Future, tuple[str, int, int]] = {}
    for trade_flow, report_val, wid in TASKS:
        fut = executor.submit(run_worker, year, month, trade_flow, report_val, active[wid], wid, str(log_file))
        pending[fut] = (trade_flow, report_val, wid)

    results: dict[int, tuple[pd.DataFrame, list[int]]] = {}

    while pending:
        done, _ = wait(list(pending.keys()), return_when=FIRST_COMPLETED)
        for fut in done:
            trade_flow, report_val, wid = pending.pop(fut)
            try:
                df, failed_ids = fut.result()
                results[wid] = (df, failed_ids)
                log.info("WORKER-%d done: %s rv=%d rows=%d failed=%d", wid, trade_flow, report_val, len(df), len(failed_ids))
            except Exception as exc:
                log.warning("WORKER-%d FAILED %04d-%02d %s rv=%d: %s", wid, year, month, trade_flow, report_val, exc)
                if not spare_pool:
                    log.error("FAILED %04d-%02d: spare proxy pool exhausted", year, month)
                    for f in list(pending.keys()):
                        f.cancel()
                    return False
                new_proxy = spare_pool.pop(0)
                log.info("WORKER-%d retrying with spare proxy (%d remain)", wid, len(spare_pool))
                new_fut = executor.submit(run_worker, year, month, trade_flow, report_val, new_proxy, wid, str(log_file))
                pending[new_fut] = (trade_flow, report_val, wid)

    ordered = [results[wid] for _, _, wid in TASKS]
    merged_df, all_failed = merge_worker_results(ordered, year, month)

    write_parquet(merged_df, year, month)
    write_checkpoint_json(year, month, merged_df, all_failed)
    log.info("COMPLETE %04d-%02d rows=%d failed_countries=%d", year, month, len(merged_df), len(all_failed))
    return True


def run_range(months: list[tuple[int, int]], log_dir: Path = LOGS_DIR) -> None:
    """Scrape specific months in-process. Skips already-complete months."""
    _, log_file = setup_logging(log_dir)
    log.info("run_range: %d months requested", len(months))
    live_proxies = get_live_proxies(PROXIES_CSV)
    with ProcessPoolExecutor(max_workers=NUM_WORKERS) as executor:
        for year, month in months:
            if is_month_complete(year, month):
                log.info("SKIP %04d-%02d: already complete", year, month)
                continue
            log.info("Processing %04d-%02d", year, month)
            try:
                run_month(year, month, live_proxies, executor, log_file)
            except Exception as exc:
                log.error("FAILED %04d-%02d: %s", year, month, exc)


def main() -> None:
    _, log_file = setup_logging(LOGS_DIR)
    log.info("=== COMEX India Historical Scraper ===")
    log.info("Period: %04d-%02d → %04d-%02d", START_YEAR, START_MONTH, END_YEAR, END_MONTH)

    live_proxies = get_live_proxies(PROXIES_CSV)

    months = generate_months(START_YEAR, START_MONTH, END_YEAR, END_MONTH)
    log.info("Total months to process: %d", len(months))

    with ProcessPoolExecutor(max_workers=NUM_WORKERS) as executor:
        for year, month in months:
            if is_month_complete(year, month):
                log.info("SKIP %04d-%02d: month already complete", year, month)
                continue
            log.info("--- Processing %04d-%02d ---", year, month)
            try:
                run_month(year, month, live_proxies, executor, log_file)
            except KeyboardInterrupt:
                log.info("interrupted, shutting down")
                break
            except Exception as exc:
                log.error("FAILED %04d-%02d unexpected: %s", year, month, exc)

    log.info("=== Scraper finished ===")


if __name__ == "__main__":
    main()
