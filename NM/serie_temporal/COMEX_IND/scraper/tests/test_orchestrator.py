import pandas as pd
import pytest

from scraper.orchestrator import generate_months, merge_worker_results


def test_generate_months_length():
    months = generate_months(2021, 1, 2026, 2)
    assert len(months) == 62


def test_generate_months_bounds():
    months = generate_months(2021, 1, 2026, 2)
    assert months[0] == (2021, 1)
    assert months[-1] == (2026, 2)


def test_generate_months_year_boundary():
    months = generate_months(2021, 11, 2022, 2)
    assert (2021, 12) in months
    assert (2022, 1) in months


def make_df(trade_flow, val_col, rows):
    data = []
    for cid, cname, hs, desc, val in rows:
        data.append({"country_id": cid, "country_name": cname, "hs_code": hs, "hs_description": desc, "raw_value": val})
    return pd.DataFrame(data, columns=["country_id", "country_name", "hs_code", "hs_description", "raw_value"])


def test_merge_worker_results_structure():
    imp_usd = make_df("IMPORT", "value_usd_million", [(77, "CHINA", "01012100", "Horses", 10.0)])
    imp_qty = make_df("IMPORT", "quantity",          [(77, "CHINA", "01012100", "Horses", 500.0)])
    exp_usd = make_df("EXPORT", "value_usd_million", [(147, "GERMANY", "02011000", "Beef", 5.0)])
    exp_qty = make_df("EXPORT", "quantity",           [(147, "GERMANY", "02011000", "Beef", 200.0)])

    results = [(imp_usd, []), (imp_qty, []), (exp_usd, []), (exp_qty, [])]
    df, failed = merge_worker_results(results, 2024, 1)

    assert set(df["trade_flow"].unique()) == {"IMPORT", "EXPORT"}
    assert "value_usd_million" in df.columns
    assert "quantity" in df.columns
    assert df["year"].iloc[0] == 2024
    assert df["month"].iloc[0] == 1
    assert failed == []


def test_merge_worker_results_failed_aggregated():
    empty = pd.DataFrame(columns=["country_id", "country_name", "hs_code", "hs_description", "raw_value"])
    results = [(empty, [1, 2]), (empty, [2, 3]), (empty, [4]), (empty, [])]
    df, failed = merge_worker_results(results, 2024, 1)
    assert failed == [1, 2, 3, 4]


def test_merge_worker_results_hs_code_padded():
    imp_usd = make_df("IMPORT", "value_usd_million", [(77, "CHINA", "1012100", "Horses", 10.0)])
    imp_qty = make_df("IMPORT", "quantity",          [(77, "CHINA", "1012100", "Horses", 500.0)])
    empty   = pd.DataFrame(columns=["country_id", "country_name", "hs_code", "hs_description", "raw_value"])
    results = [(imp_usd, []), (imp_qty, []), (empty, []), (empty, [])]
    df, _ = merge_worker_results(results, 2024, 1)
    assert df[df["trade_flow"] == "IMPORT"]["hs_code"].iloc[0] == "01012100"
