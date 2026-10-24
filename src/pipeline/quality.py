"""Stage 3 - Data quality gate on the cleaned layer.

Runs a suite of declarative checks and writes a JSON report. The pipeline
FAILS (non-zero exit / Airflow task failure) if any blocking check fails, so
bad data never reaches the warehouse. This is the same idea as Great
Expectations / dbt tests, kept dependency-free.
"""
from __future__ import annotations

import json
import logging
import sys
from dataclasses import dataclass, asdict

import pandas as pd

from . import config

log = logging.getLogger(__name__)


@dataclass
class CheckResult:
    name: str
    passed: bool
    detail: str
    blocking: bool = True


def run_checks(df: pd.DataFrame) -> list[CheckResult]:
    results: list[CheckResult] = []

    def check(name: str, passed: bool, detail: str, blocking: bool = True):
        results.append(CheckResult(name, bool(passed), detail, blocking))

    check("not_empty", len(df) > 0, f"{len(df)} rows")
    check(
        "no_null_keys",
        df[["city", "observed_at"]].notna().all().all(),
        "city & observed_at must never be null",
    )
    dups = int(df.duplicated(["city", "observed_at"]).sum())
    check("unique_city_hour", dups == 0, f"{dups} duplicate (city, hour) rows")

    for col, (lo, hi) in config.VALID_RANGES.items():
        bad = int(((df[col] < lo) | (df[col] > hi)).sum())
        check(f"range_{col}", bad == 0, f"{bad} values outside [{lo}, {hi}]")

    for col in config.VALID_RANGES:
        pct = float(df[col].isna().mean() * 100)
        check(f"null_pct_{col}", pct <= 5.0, f"{pct:.2f}% nulls (limit 5%)")

    # Freshness is a warning only: historical/sample loads are legitimately old.
    age_h = (pd.Timestamp.now() - df["observed_at"].max()).total_seconds() / 3600
    check("freshness_72h", age_h <= 72, f"newest record is {age_h:.1f}h old", blocking=False)

    # Every configured city should be present.
    missing = set(config.CITIES) - set(df["city"].unique())
    check("all_cities_present", not missing, f"missing: {sorted(missing) or 'none'}")
    return results


def run() -> list[CheckResult]:
    df = pd.read_parquet(config.CLEANED_DIR / "air_quality_clean.parquet")
    results = run_checks(df)
    config.QUALITY_REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    config.QUALITY_REPORT_PATH.write_text(json.dumps([asdict(r) for r in results], indent=2))
    for r in results:
        log.log(logging.INFO if r.passed else logging.ERROR, "%s %-22s %s",
                "PASS" if r.passed else "FAIL", r.name, r.detail)
    failed = [r for r in results if not r.passed and r.blocking]
    if failed:
        raise DataQualityError(f"{len(failed)} blocking check(s) failed: {[r.name for r in failed]}")
    return results


class DataQualityError(Exception):
    pass


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    try:
        run()
    except DataQualityError as exc:
        log.error(exc)
        sys.exit(1)
