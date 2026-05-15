import logging
import re

import requests
from bs4 import BeautifulSoup

from .constants import MONTH_NAMES, POST_TIMEOUT

log = logging.getLogger(__name__)


def _parse_value(raw: str | None) -> float | None:
    if raw is None:
        return None
    cleaned = raw.strip()
    if cleaned in ("-", "", "N.A.", "NA", "n.a."):
        return None
    cleaned = cleaned.replace(",", "")
    try:
        return float(cleaned)
    except ValueError:
        return None


def _find_value_column_index(headers: list[str], year: int) -> int:
    """Return the index of Mon-YYYY (F) for the given year, falling back to (R).

    Recent months are labelled (F); historical months are labelled (R).
    Cumulative headers like 'Jan-Mar-2021 (R)' are not matched because they
    don't fit the '^Mon-YYYY' pattern.
    """
    norm = [re.sub(r"\s+", " ", h.strip()) for h in headers]
    final_idx:   int | None = None
    revised_idx: int | None = None

    for i, h in enumerate(norm):
        for mname in MONTH_NAMES:
            if final_idx is None and re.match(
                rf"^{mname}-{year}\s*\(F\)$", h, re.IGNORECASE
            ):
                final_idx = i
            if revised_idx is None and re.match(
                rf"^{mname}-{year}\s*\(R\)$", h, re.IGNORECASE
            ):
                revised_idx = i

    result = final_idx if final_idx is not None else revised_idx
    if result is not None:
        return result
    raise ValueError(f"No 'Mon-{year} (F)/(R)' column found in headers: {headers}")


def _extract_token_from_soup(soup: BeautifulSoup, fallback: str) -> str:
    meta = soup.find("meta", attrs={"name": "csrf-token"})
    if meta and meta.get("content"):
        return meta["content"]
    hidden = soup.find("input", attrs={"name": "_token"})
    if hidden and hidden.get("value"):
        return hidden["value"]
    return fallback


def fetch_country(
    session: requests.Session,
    token: str,
    year: int,
    month: int,
    country_id: int,
    country_name: str,
    report_val: int,
    trade_flow_config: dict,
) -> tuple[list[dict], str]:
    """Returns (rows, next_token). next_token is extracted from the POST response
    and must be used for the following request to satisfy the portal's rotating CSRF."""
    payload = {
        "_token": token,
        trade_flow_config["prefix_month"]:     str(month),
        trade_flow_config["prefix_year"]:      str(year),
        trade_flow_config["prefix_country"]:   str(country_id),
        trade_flow_config["prefix_comlevel"]:  "8",
        trade_flow_config["prefix_reportval"]: str(report_val),
        trade_flow_config["prefix_reportyr"]:  "2",
    }

    resp = session.post(
        trade_flow_config["url"],
        data=payload,
        timeout=POST_TIMEOUT,
        verify=False,
    )

    soup = BeautifulSoup(resp.text, "html.parser")
    next_token = _extract_token_from_soup(soup, fallback=token)

    resp.raise_for_status()

    table = soup.find("table", id="example1")
    if table is None:
        all_tables = soup.find_all("table")
        log.warning(
            "country=%d no table#example1 (status=%d); found %d table(s): %s",
            country_id, resp.status_code, len(all_tables),
            [t.get("id") or t.get("class") for t in all_tables],
        )
        return [], next_token

    rows_all = table.find_all("tr")
    if len(rows_all) < 2:
        return [], next_token

    header_cells = rows_all[0].find_all(["th", "td"])
    headers = [c.get_text(strip=True) for c in header_cells]
    norm = [re.sub(r"\s+", " ", h.strip()) for h in headers]

    try:
        val_col = _find_value_column_index(headers, year)
    except ValueError:
        log.warning("country=%d no (F)/(R) column for %d; headers=%s", country_id, year, headers)
        return [], next_token

    unit_col = next((i for i, h in enumerate(norm) if h.lower() == "unit"), None)

    results: list[dict] = []
    for tr in rows_all[1:]:
        cells = tr.find_all("td")
        if len(cells) <= val_col:
            continue
        hs_raw = cells[1].get_text(strip=True) if len(cells) > 1 else ""
        desc = cells[2].get_text(strip=True) if len(cells) > 2 else ""
        raw_val = cells[val_col].get_text(strip=True)
        unit_val = cells[unit_col].get_text(strip=True) if unit_col is not None and len(cells) > unit_col else None

        hs_code = hs_raw.strip().zfill(8)
        if not hs_code or hs_code == "00000000":
            continue

        results.append({
            "country_id":     country_id,
            "country_name":   country_name,
            "hs_code":        hs_code,
            "hs_description": desc,
            "raw_value":      _parse_value(raw_val),
            "unit":           unit_val or None,
        })

    return results, next_token
