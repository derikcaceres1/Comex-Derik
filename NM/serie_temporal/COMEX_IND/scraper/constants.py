from pathlib import Path

_PKG_ROOT: Path = Path(__file__).resolve().parent
_IND_DATA_ROOT: Path = _PKG_ROOT.parent.parent.parent / "dados" / "IND"

PARQUET_BASE: Path = _IND_DATA_ROOT / "scraper_parquet"
LOGS_DIR: Path = _IND_DATA_ROOT / "logs"
PROXIES_CSV: Path = _IND_DATA_ROOT / "proxies.csv"

IMPORT_URL: str = "https://tradestat.commerce.gov.in/meidb/country_wise_all_commodities_import"
EXPORT_URL: str = "https://tradestat.commerce.gov.in/meidb/country_wise_all_commodities_export"

TRADE_FLOWS: dict = {
    "IMPORT": {
        "url": IMPORT_URL,
        "prefix_month":     "cwcimMonth",
        "prefix_year":      "cwcimYear",
        "prefix_country":   "cwcimallcount",
        "prefix_comlevel":  "cwcimCommodityLevel",
        "prefix_reportval": "cwcimReportVal",
        "prefix_reportyr":  "cwcimReportYear",
        "select_id":        "cwcimallcount",
    },
    "EXPORT": {
        "url": EXPORT_URL,
        "prefix_month":     "cwcexddMonth",
        "prefix_year":      "cwcexddYear",
        "prefix_country":   "cwcexallcount",
        "prefix_comlevel":  "cwcexddCommodityLevel",
        "prefix_reportval": "cwcexddReportVal",
        "prefix_reportyr":  "cwcexddReportYear",
        "select_id":        "cwcexallcount",
    },
}

REPORT_VAL_USD: int = 1
REPORT_VAL_QTY: int = 2

START_YEAR: int = 2021
START_MONTH: int = 1
END_YEAR: int = 2026
END_MONTH: int = 2

GET_TIMEOUT: int = 60
POST_TIMEOUT: int = 90
PROXY_TEST_TIMEOUT: int = 10

RETRY_TOTAL: int = 5
RETRY_BACKOFF_FACTOR: int = 5
RETRY_STATUS_FORCELIST: list = [500, 502, 503, 504]

REQUEST_DELAY_MIN: float = 0.6
REQUEST_DELAY_MAX: float = 1.4
NETWORK_RETRY_SLEEP: int = 30

CHROME_HEADERS: dict = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
}

EXPECTED_COLUMNS: list = [
    "year", "month", "trade_flow", "country_id", "country_name",
    "hs_code", "hs_description", "value_usd_million", "quantity", "quantity_unit",
]

PARQUET_COMPRESSION: str = "snappy"
PARQUET_ENGINE: str = "pyarrow"

MONTH_NAMES: list = [
    "Jan", "Feb", "Mar", "Apr", "May", "Jun",
    "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
]

LOG_FORMAT_MAIN: str = "%(asctime)s [MAIN] %(levelname)s %(message)s"
LOG_FORMAT_WORKER: str = "%(asctime)s [WORKER-{worker_id}] %(levelname)s %(message)s"

NUM_WORKERS: int = 4
MIN_LIVE_PROXIES: int = 4
