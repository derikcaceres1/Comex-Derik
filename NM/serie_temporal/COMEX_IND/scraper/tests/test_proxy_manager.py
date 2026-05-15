from pathlib import Path
from unittest.mock import MagicMock, patch
import pandas as pd
import pytest

from scraper.proxy_manager import load_and_test_proxies


def make_proxies_csv(tmp_path, rows):
    df = pd.DataFrame(rows)
    csv_path = tmp_path / "proxies.csv"
    df.to_csv(csv_path, index=False)
    return csv_path


def test_load_and_test_proxies_filters_dead(tmp_path):
    rows = [
        {"valid": True, "proxy_http": "http://a:p@1.2.3.4:8080", "proxy_https": "http://a:p@1.2.3.4:8080"},
        {"valid": True, "proxy_http": "http://a:p@5.6.7.8:8080", "proxy_https": "http://a:p@5.6.7.8:8080"},
    ]
    csv_path = make_proxies_csv(tmp_path, rows)

    responses = [MagicMock(status_code=200), MagicMock(status_code=407)]

    with patch("scraper.proxy_manager.requests.get", side_effect=responses):
        live = load_and_test_proxies(csv_path, "http://test.com", timeout=5)

    assert len(live) == 1
    assert live[0]["http"] == "http://a:p@1.2.3.4:8080"


def test_load_and_test_proxies_all_dead(tmp_path):
    rows = [{"valid": True, "proxy_http": "http://a:p@1.2.3.4:8080", "proxy_https": "http://a:p@1.2.3.4:8080"}]
    csv_path = make_proxies_csv(tmp_path, rows)

    with patch("scraper.proxy_manager.requests.get", side_effect=Exception("timeout")):
        live = load_and_test_proxies(csv_path, "http://test.com", timeout=5)

    assert live == []


def test_load_and_test_proxies_preserves_order(tmp_path):
    rows = [
        {"valid": True, "proxy_http": f"http://a:p@{i}.0.0.1:80", "proxy_https": f"http://a:p@{i}.0.0.1:80"}
        for i in range(5)
    ]
    csv_path = make_proxies_csv(tmp_path, rows)

    with patch("scraper.proxy_manager.requests.get", return_value=MagicMock(status_code=200)):
        live = load_and_test_proxies(csv_path, "http://test.com", timeout=5)

    assert len(live) == 5
    assert live[0]["http"] == "http://a:p@0.0.0.1:80"
