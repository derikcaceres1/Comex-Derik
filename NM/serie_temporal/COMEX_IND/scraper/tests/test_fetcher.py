import pytest
from unittest.mock import MagicMock

from scraper.fetcher import _parse_value, _find_value_column_index, fetch_country


@pytest.mark.parametrize("raw,expected", [
    ("-", None),
    ("", None),
    ("N.A.", None),
    (None, None),
    ("0", 0.0),
    ("1,234.56", 1234.56),
    ("100", 100.0),
    ("0.5", 0.5),
])
def test_parse_value(raw, expected):
    assert _parse_value(raw) == expected


@pytest.mark.parametrize("headers,year,expected_idx", [
    (["S.No.", "HSCode", "Commodity", "Jan-2024 (F)", "%Growth"], 2024, 3),
    (["S.No.", "HSCode", "Commodity", "Feb-2025 (R)", "Feb-2026 (F)", "%Growth"], 2026, 4),
    (["S.No.", "HSCode", "Commodity", "Jan-2020 (R)", "Jan-2021 (R)", "%Growth", "Jan-Jan-2020 (R)", "Jan-Jan-2021 (R)", "%Growth"], 2021, 4),
    (["S.No.", "HSCode", "Commodity", "Unit", "Jan-2021 (R)", "%Growth"], 2021, 4),
    (["S.No.", "HSCode", "Commodity", "Jan-2021 (R)", "Jan-Jan-2021 (R)", "%Growth"], 2021, 3),
])
def test_find_value_column_index(headers, year, expected_idx):
    assert _find_value_column_index(headers, year) == expected_idx


def test_find_value_column_raises_when_missing():
    with pytest.raises(ValueError):
        _find_value_column_index(["S.No.", "HSCode", "Commodity"], 2024)


TF_CFG = {
    "url": "http://example.com",
    "prefix_month": "m", "prefix_year": "y", "prefix_country": "c",
    "prefix_comlevel": "l", "prefix_reportval": "rv", "prefix_reportyr": "ry",
}


def _mock_response(html: str):
    response = MagicMock()
    response.text = html
    response.raise_for_status = MagicMock()
    return response


def test_fetch_country_no_table():
    session = MagicMock()
    session.post.return_value = _mock_response("<html><body>No table here</body></html>")
    rows, next_token = fetch_country(session, "tok1", 2024, 1, 1, "COUNTRY", 1, TF_CFG)
    assert rows == []
    assert next_token == "tok1"


def test_fetch_country_parses_rows():
    html = """
    <html><body>
    <form><input type="hidden" name="_token" value="tok2"/></form>
    <table id="example1">
      <tr><th>S.No</th><th>HSCode</th><th>Commodity</th><th>Jan-2024 (F)</th></tr>
      <tr><td>1</td><td>01012100</td><td>Live horses</td><td>1,234.56</td></tr>
      <tr><td>2</td><td>02011000</td><td>Fresh beef</td><td>-</td></tr>
    </table>
    </body></html>
    """
    session = MagicMock()
    session.post.return_value = _mock_response(html)
    rows, next_token = fetch_country(session, "tok1", 2024, 1, 77, "CHINA", 1, TF_CFG)
    assert len(rows) == 2
    assert rows[0]["hs_code"] == "01012100"
    assert rows[0]["raw_value"] == pytest.approx(1234.56)
    assert rows[1]["raw_value"] is None
    assert next_token == "tok2"


def test_fetch_country_token_chained():
    """next_token from response is returned even when table is empty."""
    html = '<html><body><input name="_token" value="fresh_tok"/></body></html>'
    session = MagicMock()
    session.post.return_value = _mock_response(html)
    rows, next_token = fetch_country(session, "old_tok", 2024, 1, 1, "TEST", 1, TF_CFG)
    assert rows == []
    assert next_token == "fresh_tok"


def test_hs_code_padding():
    html = """
    <html><body>
    <table id="example1">
      <tr><th>S.No</th><th>HSCode</th><th>Commodity</th><th>Jan-2024 (F)</th></tr>
      <tr><td>1</td><td>1012100</td><td>Horses</td><td>10.0</td></tr>
    </table>
    </body></html>
    """
    session = MagicMock()
    session.post.return_value = _mock_response(html)
    rows, _ = fetch_country(session, "tok1", 2024, 1, 1, "TEST", 1, TF_CFG)
    assert rows[0]["hs_code"] == "01012100"
