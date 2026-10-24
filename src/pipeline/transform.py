"""Stage 2 - Transformation: raw -> cleaned.

Cleaning rules (each is counted so the run is auditable):
  * parse timestamps, drop rows with unparseable time
  * drop exact duplicates on (city, observed_at), keeping the latest ingest
  * null-out physically impossible values (negative / above valid range)
  * fill short gaps by linear interpolation per city (max 3 consecutive hours)
  * derive Indian AQI category from PM2.5 (CPCB breakpoints)
"""
from __future__ import annotations

import json
import logging

import pandas as pd

from . import config

log = logging.getLogger(__name__)

POLLUTANTS = list(config.VALID_RANGES)

# CPCB PM2.5 24h breakpoints (ug/m3) -> category
PM25_BANDS = [
    (30, "Good"),
    (60, "Satisfactory"),
    (90, "Moderate"),
    (120, "Poor"),
    (250, "Very Poor"),
    (float("inf"), "Severe"),
]


def aqi_category(pm25: float) -> str | None:
    if pd.isna(pm25):
        return None
    for upper, label in PM25_BANDS:
        if pm25 <= upper:
            return label
    return None


def load_raw() -> pd.DataFrame:
    files = sorted(config.RAW_DIR.glob("ingest_date=*/*.parquet"))
    if not files:
        raise FileNotFoundError(f"no raw files under {config.RAW_DIR}")
    return pd.concat((pd.read_parquet(f) for f in files), ignore_index=True)


def clean(df: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    stats = {"rows_in": len(df)}

    df = df.copy()
    df["observed_at"] = pd.to_datetime(df["observed_at"], errors="coerce")
    stats["bad_timestamps"] = int(df["observed_at"].isna().sum())
    df = df.dropna(subset=["observed_at", "city"])

    before = len(df)
    df = df.sort_values("_ingested_at").drop_duplicates(["city", "observed_at"], keep="last")
    stats["duplicates_removed"] = before - len(df)

    out_of_range = 0
    for col, (lo, hi) in config.VALID_RANGES.items():
        bad = (df[col] < lo) | (df[col] > hi)
        out_of_range += int(bad.sum())
        df.loc[bad, col] = pd.NA
        df[col] = pd.to_numeric(df[col], errors="coerce")
    stats["out_of_range_nulled"] = out_of_range

    df = df.sort_values(["city", "observed_at"])
    nulls_before = int(df[POLLUTANTS].isna().sum().sum())
    df[POLLUTANTS] = df.groupby("city")[POLLUTANTS].transform(
        lambda s: s.interpolate(limit=3, limit_direction="both")
    )
    stats["nulls_filled_by_interpolation"] = nulls_before - int(df[POLLUTANTS].isna().sum().sum())

    df["aqi_category"] = df["pm2_5"].map(aqi_category)
    df = df[["city", "observed_at", *POLLUTANTS, "aqi_category"]].reset_index(drop=True)
    stats["rows_out"] = len(df)
    return df, stats


def run() -> dict:
    cleaned, stats = clean(load_raw())
    config.CLEANED_DIR.mkdir(parents=True, exist_ok=True)
    out = config.CLEANED_DIR / "air_quality_clean.parquet"
    cleaned.to_parquet(out, index=False)
    log.info("cleaned: %s %s", out.name, json.dumps(stats))
    return stats


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    run()
