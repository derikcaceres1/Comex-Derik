import json
import pandas as pd
import pytest

from scraper.constants import EXPECTED_COLUMNS


def make_test_df(year=2024, month=1):
    return pd.DataFrame({
        "year": pd.array([year], dtype="int16"),
        "month": pd.array([month], dtype="int8"),
        "trade_flow": pd.Categorical(["IMPORT"], categories=["IMPORT", "EXPORT"]),
        "country_id": pd.array([77], dtype="int16"),
        "country_name": pd.array(["CHINA"], dtype="string"),
        "hs_code": pd.array(["01012100"], dtype="string"),
        "hs_description": pd.array(["Horses"], dtype="string"),
        "value_usd_million": pd.array([1.5], dtype="float32"),
        "quantity": pd.array([100.0], dtype="float32"),
        "quantity_unit": pd.array([None], dtype="string"),
    })


def test_is_month_complete_missing(tmp_path, monkeypatch):
    import scraper.checkpoint as ckpt
    monkeypatch.setattr(ckpt, "PARQUET_BASE", tmp_path)
    assert ckpt.is_month_complete(2024, 1) is False


def test_is_month_complete_valid(tmp_path, monkeypatch):
    import scraper.checkpoint as ckpt
    monkeypatch.setattr(ckpt, "PARQUET_BASE", tmp_path)

    df = make_test_df()
    ckpt.write_parquet(df, 2024, 1)
    assert ckpt.is_month_complete(2024, 1) is True


def test_is_month_complete_bad_schema(tmp_path, monkeypatch):
    import scraper.checkpoint as ckpt
    monkeypatch.setattr(ckpt, "PARQUET_BASE", tmp_path)

    path = ckpt.parquet_path(2024, 1)
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame({"year": [2024]}).to_parquet(path)
    assert ckpt.is_month_complete(2024, 1) is False


def test_write_parquet_atomic(tmp_path, monkeypatch):
    import scraper.checkpoint as ckpt
    monkeypatch.setattr(ckpt, "PARQUET_BASE", tmp_path)

    df = make_test_df()
    ckpt.write_parquet(df, 2024, 1)

    path = ckpt.parquet_path(2024, 1)
    assert path.exists()
    tmp = path.with_suffix(".tmp.parquet")
    assert not tmp.exists()

    result = pd.read_parquet(path)
    for col in EXPECTED_COLUMNS:
        assert col in result.columns


def test_write_checkpoint_json(tmp_path, monkeypatch):
    import scraper.checkpoint as ckpt
    monkeypatch.setattr(ckpt, "PARQUET_BASE", tmp_path)

    df = make_test_df()
    ckpt.write_parquet(df, 2024, 1)
    ckpt.write_checkpoint_json(2024, 1, df, [5, 10])

    json_path = ckpt.parquet_path(2024, 1).with_suffix(".json")
    assert json_path.exists()
    meta = json.loads(json_path.read_text())
    assert meta["year"] == 2024
    assert meta["month"] == 1
    assert meta["row_count"] == 1
    assert meta["failed_country_ids"] == [5, 10]
