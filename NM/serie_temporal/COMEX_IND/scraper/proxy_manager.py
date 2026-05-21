import warnings
from pathlib import Path

import pandas as pd
import requests
from urllib3.exceptions import InsecureRequestWarning

warnings.simplefilter("ignore", InsecureRequestWarning)


def load_and_test_proxies(
    proxies_csv_path: Path,
    test_url: str,
    timeout: int = 10,
) -> list[dict]:
    df = pd.read_csv(proxies_csv_path)
    df = df[df["valid"] == True]

    live: list[dict] = []
    for row in df.itertuples(index=False):
        proxy = {"http": row.proxy_http, "https": row.proxy_https}
        try:
            resp = requests.get(test_url, proxies=proxy, timeout=timeout, verify=False)
            if resp.status_code == 200:
                live.append(proxy)
        except Exception:
            pass
    return live
