import duckdb
import numpy as np
import pandas as pd
import pytest

from pipeline import config, ingest, load, quality, transform
from pipeline.run_pipeline import main as run_all


@pytest.fixture()
def data_dir(tmp_path, monkeypatch):
    """Point every pipeline path at an isolated temp folder."""
    monkeypatch.setattr(config, "DATA_DIR", tmp_path)
    monkeypatch.setattr(config, "RAW_DIR", tmp_path / "raw")
    monkeypatch.setattr(config, "CLEANED_DIR", tmp_path / "cleaned")
    monkeypatch.setattr(config, "CURATED_DIR", tmp_path / "curated")
    monkeypatch.setattr(config, "WAREHOUSE_PATH", tmp_path / "warehouse.duckdb")
    monkeypatch.setattr(config, "QUALITY_REPORT_PATH", tmp_path / "quality_report.json")
    return tmp_path


def _raw_frame(**overrides):
    base = pd.DataFrame(
        {
            "observed_at": ["2026-01-01T00:00", "2026-01-01T01:00", "2026-01-01T02:00"],
            "city": ["Delhi"] * 3,
            "pm2_5": [10.0, 20.0, 30.0],
            "pm10": [20.0, 30.0, 40.0],
            "carbon_monoxide": [300.0, 310.0, 320.0],
            "nitrogen_dioxide": [10.0, 11.0, 12.0],
            "ozone": [40.0, 41.0, 42.0],
            "_ingested_at": ["2026-01-02T00:00:00+00:00"] * 3,
        }
    )
    for k, v in overrides.items():
        base[k] = v
    return base


# ---------------------------------------------------------------- transform
def test_aqi_category_bands():
    assert transform.aqi_category(10) == "Good"
    assert transform.aqi_category(75) == "Moderate"
    assert transform.aqi_category(500) == "Severe"
    assert transform.aqi_category(float("nan")) is None


def test_clean_removes_duplicates_keeping_latest():
    df = pd.concat([_raw_frame(), _raw_frame(pm2_5=[99.0, 20.0, 30.0], _ingested_at="2026-01-03T00:00:00+00:00")])
    cleaned, stats = transform.clean(df)
    assert stats["duplicates_removed"] == 3
    assert len(cleaned) == 3
    assert cleaned.loc[cleaned["observed_at"] == "2026-01-01 00:00", "pm2_5"].iloc[0] == 99.0


def test_clean_nulls_impossible_values_then_interpolates():
    df = _raw_frame(pm2_5=[10.0, -50.0, 30.0])
    cleaned, stats = transform.clean(df)
    assert stats["out_of_range_nulled"] == 1
    assert cleaned["pm2_5"].tolist() == [10.0, 20.0, 30.0]  # filled between neighbours


def test_clean_drops_bad_timestamps():
    df = _raw_frame(observed_at=["2026-01-01T00:00", "not-a-date", "2026-01-01T02:00"])
    cleaned, stats = transform.clean(df)
    assert stats["bad_timestamps"] == 1
    assert len(cleaned) == 2


# ------------------------------------------------------------------ quality
def _clean_frame(n=10):
    return pd.DataFrame(
        {
            "city": list(config.CITIES)[0],
            "observed_at": pd.date_range(pd.Timestamp.now().floor("h"), periods=n, freq="h"),
            "pm2_5": 20.0, "pm10": 30.0, "carbon_monoxide": 300.0,
            "nitrogen_dioxide": 10.0, "ozone": 40.0,
        }
    )


def test_quality_flags_out_of_range_and_missing_cities():
    df = _clean_frame()
    df.loc[0, "pm2_5"] = -1
    by_name = {r.name: r for r in quality.run_checks(df)}
    assert not by_name["range_pm2_5"].passed
    assert not by_name["all_cities_present"].passed  # only one city in the frame


def test_quality_flags_excess_nulls_and_duplicates():
    df = _clean_frame(20)
    df.loc[:4, "ozone"] = np.nan
    df = pd.concat([df, df.iloc[[0]]])
    by_name = {r.name: r for r in quality.run_checks(df)}
    assert not by_name["null_pct_ozone"].passed
    assert not by_name["unique_city_hour"].passed


def test_stale_data_is_warning_not_blocking():
    df = _clean_frame()
    df["observed_at"] = df["observed_at"] - pd.Timedelta(days=30)
    fresh = {r.name: r for r in quality.run_checks(df)}["freshness_72h"]
    assert not fresh.passed and not fresh.blocking


# ---------------------------------------------------------------- end to end
def test_full_pipeline_builds_star_schema(data_dir):
    run_all("sample")
    con = duckdb.connect(str(config.WAREHOUSE_PATH), read_only=True)
    assert con.execute("SELECT COUNT(*) FROM dim_city").fetchone()[0] == len(config.CITIES)
    n_fact = con.execute("SELECT COUNT(*) FROM fact_air_quality").fetchone()[0]
    assert n_fact > 0
    # referential integrity: no orphan keys
    orphans = con.execute(
        "SELECT COUNT(*) FROM fact_air_quality f "
        "LEFT JOIN dim_city c USING (city_key) LEFT JOIN dim_time t USING (time_key) "
        "WHERE c.city_key IS NULL OR t.time_key IS NULL"
    ).fetchone()[0]
    assert orphans == 0
    # fact grain is unique
    dup = con.execute(
        "SELECT COUNT(*) FROM (SELECT city_key, time_key FROM fact_air_quality "
        "GROUP BY 1, 2 HAVING COUNT(*) > 1)"
    ).fetchone()[0]
    assert dup == 0
    assert (config.CURATED_DIR / "mart_city_ranking.parquet").exists()


def test_pipeline_is_idempotent(data_dir):
    run_all("sample")
    first = duckdb.connect(str(config.WAREHOUSE_PATH), read_only=True).execute(
        "SELECT COUNT(*) FROM fact_air_quality").fetchone()[0]
    run_all("sample")
    second = duckdb.connect(str(config.WAREHOUSE_PATH), read_only=True).execute(
        "SELECT COUNT(*) FROM fact_air_quality").fetchone()[0]
    assert first == second


def test_quality_gate_blocks_bad_data_from_warehouse(data_dir, monkeypatch):
    ingest.ingest("sample")
    transform.run()
    path = config.CLEANED_DIR / "air_quality_clean.parquet"
    df = pd.read_parquet(path)
    df.loc[df.index[:200], "pm10"] = np.nan  # blow through the 5% null limit
    df.to_parquet(path, index=False)
    with pytest.raises(quality.DataQualityError):
        quality.run()
    assert not config.WAREHOUSE_PATH.exists()  # load never ran
