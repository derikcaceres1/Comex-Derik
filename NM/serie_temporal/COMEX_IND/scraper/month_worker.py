import logging
import random
import time
from logging.handlers import RotatingFileHandler
from pathlib import Path

import pandas as pd
import requests

from .constants import (
    LOG_FORMAT_WORKER,
    NETWORK_RETRY_SLEEP,
    REQUEST_DELAY_MAX,
    REQUEST_DELAY_MIN,
    TRADE_FLOWS,
)
from .fetcher import fetch_country
from .session_manager import build_session


def _setup_worker_logger(worker_id: int, log_file: str) -> logging.Logger:
    logger = logging.getLogger(f"worker.{worker_id}")
    logger.setLevel(logging.DEBUG)
    if logger.handlers:
        return logger
    logger.propagate = False

    fmt = logging.Formatter(
        LOG_FORMAT_WORKER.format(worker_id=worker_id),
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    fh = RotatingFileHandler(
        Path(log_file), maxBytes=50 * 1024 * 1024, backupCount=5, encoding="utf-8"
    )
    fh.setFormatter(fmt)
    logger.addHandler(fh)
    return logger


def run_worker(
    year: int,
    month: int,
    trade_flow: str,
    report_val: int,
    proxy: dict,
    worker_id: int,
    log_file: str,
) -> tuple[pd.DataFrame, list[int]]:
    log = _setup_worker_logger(worker_id, log_file)
    log.info("start year=%d month=%02d trade_flow=%s rv=%d", year, month, trade_flow, report_val)

    tf_cfg = TRADE_FLOWS[trade_flow]
    session, token, country_map = build_session(tf_cfg["url"], proxy=proxy)
    log.info("session ready countries=%d", len(country_map))

    rows: list[dict] = []
    failed: list[int] = []
    total = len(country_map)

    for i, (cid, cname) in enumerate(country_map.items(), 1):
        try:
            country_rows, token = fetch_country(session, token, year, month, cid, cname, report_val, tf_cfg)
            rows.extend(country_rows)
        except requests.exceptions.RequestException as exc:
            log.warning("network err country=%d (%s): %s — sleep %ds retry", cid, cname, exc, NETWORK_RETRY_SLEEP)
            time.sleep(NETWORK_RETRY_SLEEP)
            try:
                country_rows, token = fetch_country(session, token, year, month, cid, cname, report_val, tf_cfg)
                rows.extend(country_rows)
            except requests.exceptions.RequestException as exc2:
                log.warning("country=%d (%s) failed after retry: %s — skip", cid, cname, exc2)
                failed.append(cid)
                time.sleep(random.uniform(REQUEST_DELAY_MIN, REQUEST_DELAY_MAX))
                continue
        except ValueError as exc:
            log.debug("country=%d (%s) no matching column: %s", cid, cname, exc)

        time.sleep(random.uniform(REQUEST_DELAY_MIN, REQUEST_DELAY_MAX))

        if i % 20 == 0 or i == total:
            log.info("progress %d/%d (%.1f%%) rows_so_far=%d", i, total, 100 * i / total, len(rows))

    _cols = ["country_id", "country_name", "hs_code", "hs_description", "raw_value", "unit"]
    df = pd.DataFrame(rows if rows else [], columns=_cols)

    log.info("done rows=%d failed=%d", len(df), len(failed))
    return df, failed
