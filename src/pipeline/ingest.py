"""Stage 1 - Ingestion: land immutable, partitioned raw files.

Two sources are supported:
  * ``api``    - pulls hourly air-quality readings from the free Open-Meteo API.
  * ``sample`` - deterministic synthetic data (works offline, used by tests/CI).

Raw data is written as Parquet partitioned by ingestion date, exactly as it
would be laid out in S3:  raw/ingest_date=YYYY-MM-DD/<city>.parquet
Raw files are never modified after landing (append-only), so any downstream
step can be replayed from them.
"""
from __future__ import annotations

import argparse
import logging
import zlib
from datetime import date, datetime, timezone

import numpy as np
import pandas as pd
import requests

from . import config

log = logging.getLogger(__name__)


def fetch_from_api(city: str, meta: dict, past_days: int = 3) -> pd.DataFrame:
    params = {
        "latitude": meta["lat"],
        "longitude": meta["lon"],
        "hourly": ",".join(config.API_HOURLY_FIELDS),
        "past_days": past_days,
        "timezone": "Asia/Kolkata",
    }
    resp = requests.get(config.API_URL, params=params, timeout=30)
    resp.raise_for_status()
    hourly = resp.json()["hourly"]
    df = pd.DataFrame(hourly).rename(columns={"time": "observed_at"})
    df["city"] = city
    return df


def generate_sample(city: str, meta: dict, hours: int = 72, seed: int = 42) -> pd.DataFrame:
    """Synthetic readings with daily seasonality, city bias, noise and a few
    deliberately dirty rows (nulls, negatives, duplicates) so the cleaning and
    data-quality stages have real work to do."""
    rng = np.random.default_rng(zlib.crc32(f"{city}-{seed}".encode()))
    # Anchor to midnight so sample runs are reproducible within a day.
    end = pd.Timestamp.now().normalize()
    idx = pd.date_range(end=end, periods=hours, freq="h")
    hour = idx.hour.to_numpy()
    rush = np.exp(-((hour - 9) ** 2) / 8) + np.exp(-((hour - 19) ** 2) / 8)
    city_bias = {"Delhi": 2.2, "Kolkata": 1.5, "Mumbai": 1.2, "Hassan": 0.5}.get(city, 1.0)
    pm25 = (25 + 40 * rush) * city_bias + rng.normal(0, 5, hours)
    df = pd.DataFrame(
        {
            "observed_at": idx.strftime("%Y-%m-%dT%H:%M"),
            "pm2_5": pm25,
            "pm10": pm25 * 1.6 + rng.normal(0, 8, hours),
            "carbon_monoxide": 300 + 250 * rush * city_bias + rng.normal(0, 30, hours),
            "nitrogen_dioxide": 15 + 25 * rush * city_bias + rng.normal(0, 4, hours),
            "ozone": 40 + 30 * np.sin((hour - 6) / 24 * 2 * np.pi).clip(0) + rng.normal(0, 5, hours),
        }
    )
    df["city"] = city
    # Inject dirty data (~3%).
    n = len(df)
    df.loc[rng.choice(n, 2, replace=False), "pm2_5"] = np.nan
    df.loc[rng.choice(n, 1, replace=False), "ozone"] = -5.0
    df = pd.concat([df, df.sample(2, random_state=1)], ignore_index=True)
    return df


def ingest(source: str = "sample", run_date: date | None = None) -> list[str]:
    run_date = run_date or date.today()
    out_dir = config.RAW_DIR / f"ingest_date={run_date.isoformat()}"
    out_dir.mkdir(parents=True, exist_ok=True)
    written = []
    for city, meta in config.CITIES.items():
        try:
            df = fetch_from_api(city, meta) if source == "api" else generate_sample(city, meta)
        except requests.RequestException as exc:  # one bad city must not kill the run
            log.error("ingest failed for %s: %s", city, exc)
            continue
        df["_ingested_at"] = datetime.now(timezone.utc).isoformat()
        df["_source"] = source
        path = out_dir / f"{city.lower()}.parquet"
        df.to_parquet(path, index=False)
        written.append(str(path))
        log.info("raw: %s rows=%d", path.name, len(df))
    if not written:
        raise RuntimeError("ingestion produced no files")
    return written


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", choices=["api", "sample"], default="sample")
    ingest(ap.parse_args().source)
