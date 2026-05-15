import warnings

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.exceptions import InsecureRequestWarning
from urllib3.util.retry import Retry

from .constants import (
    CHROME_HEADERS,
    GET_TIMEOUT,
    RETRY_BACKOFF_FACTOR,
    RETRY_STATUS_FORCELIST,
    RETRY_TOTAL,
)

warnings.simplefilter("ignore", InsecureRequestWarning)


def build_session(
    url: str,
    proxy: dict | None = None,
) -> tuple[requests.Session, str, dict[int, str]]:
    session = requests.Session()
    session.headers.update(CHROME_HEADERS)
    session.verify = False
    if proxy is not None:
        session.proxies.update(proxy)

    retry = Retry(
        total=RETRY_TOTAL,
        backoff_factor=RETRY_BACKOFF_FACTOR,
        status_forcelist=RETRY_STATUS_FORCELIST,
        allowed_methods=frozenset(["GET", "POST"]),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)

    resp = session.get(url, timeout=GET_TIMEOUT)
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "html.parser")

    token: str | None = None
    meta = soup.find("meta", attrs={"name": "csrf-token"})
    if meta and meta.get("content"):
        token = meta["content"]
    if token is None:
        hidden = soup.find("input", attrs={"name": "_token"})
        if hidden and hidden.get("value"):
            token = hidden["value"]
    if token is None:
        raise RuntimeError("CSRF token not found on landing page")

    select = soup.find("select", id="cwcimallcount") or soup.find("select", id="cwcexallcount")
    if select is None:
        for tag in soup.find_all("select"):
            sid = tag.get("id", "")
            if "count" in sid.lower():
                select = tag
                break
    if select is None:
        raise RuntimeError("country <select> not found on landing page")

    country_map: dict[int, str] = {}
    for opt in select.find_all("option"):
        val = (opt.get("value") or "").strip()
        name = opt.get_text(strip=True)
        if not val or not name:
            continue
        try:
            cid = int(val)
            if cid not in country_map:
                country_map[cid] = name
        except ValueError:
            continue

    if not country_map:
        raise RuntimeError("country_map empty after parsing landing page")

    return session, token, country_map
