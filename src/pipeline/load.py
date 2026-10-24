"""Stage 4 - Load: cleaned layer -> star-schema warehouse (curated).

Idempotent: tables are rebuilt from the cleaned Parquet each run, so re-running
a day never creates duplicates. Curated aggregates are also exported as
Parquet so Athena/QuickSight can read them straight from S3.
"""
from __future__ import annotations

import logging

import duckdb
import pandas as pd

from . import config

log = logging.getLogger(__name__)
SCHEMA_SQL = config.PROJECT_ROOT / "sql" / "schema.sql"


def build_dim_time(observed: pd.Series) -> pd.DataFrame:
    t = pd.DataFrame({"observed_at": pd.to_datetime(observed.unique())}).sort_values("observed_at")
    t["time_key"] = t["observed_at"].dt.strftime("%Y%m%d%H").astype("int64")
    t["date"] = t["observed_at"].dt.date
    t["year"] = t["observed_at"].dt.year
    t["month"] = t["observed_at"].dt.month
    t["day"] = t["observed_at"].dt.day
    t["hour"] = t["observed_at"].dt.hour
    t["day_of_week"] = t["observed_at"].dt.day_name()
    t["is_weekend"] = t["observed_at"].dt.dayofweek >= 5
    t["time_of_day"] = pd.cut(
        t["hour"], bins=[-1, 5, 11, 16, 20, 23],
        labels=["Night", "Morning", "Afternoon", "Evening", "Night "],
    ).astype(str).str.strip()
    return t


def build_dim_city() -> pd.DataFrame:
    rows = [
        {"city_key": i + 1, "city_name": name, "state": m["state"],
         "latitude": m["lat"], "longitude": m["lon"]}
        for i, (name, m) in enumerate(config.CITIES.items())
    ]
    return pd.DataFrame(rows)


def run() -> dict:
    clean = pd.read_parquet(config.CLEANED_DIR / "air_quality_clean.parquet")
    dim_city, dim_time = build_dim_city(), build_dim_time(clean["observed_at"])

    fact = clean.merge(dim_city[["city_key", "city_name"]], left_on="city", right_on="city_name")
    fact["time_key"] = fact["observed_at"].dt.strftime("%Y%m%d%H").astype("int64")
    fact = fact[["city_key", "time_key", "pm2_5", "pm10", "carbon_monoxide",
                 "nitrogen_dioxide", "ozone", "aqi_category"]]

    config.DATA_DIR.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(config.WAREHOUSE_PATH))
    con.execute(SCHEMA_SQL.read_text())
    con.register("d_city", dim_city)
    con.register("d_time", dim_time)
    con.register("f_air", fact)
    con.execute("INSERT INTO dim_city SELECT * FROM d_city")
    con.execute("INSERT INTO dim_time SELECT time_key, observed_at, date, year, month, day, hour, "
                "day_of_week, is_weekend, time_of_day FROM d_time")
    con.execute("INSERT INTO fact_air_quality SELECT * FROM f_air")

    # Curated, query-ready aggregates (also exported as Parquet for Athena).
    config.CURATED_DIR.mkdir(parents=True, exist_ok=True)
    marts = {
        "mart_city_daily": """
            SELECT c.city_name, t.date,
                   ROUND(AVG(f.pm2_5), 2) AS avg_pm25, ROUND(MAX(f.pm2_5), 2) AS max_pm25,
                   ROUND(AVG(f.pm10), 2) AS avg_pm10, ROUND(AVG(f.nitrogen_dioxide), 2) AS avg_no2,
                   COUNT(*) AS readings
            FROM fact_air_quality f
            JOIN dim_city c USING (city_key) JOIN dim_time t USING (time_key)
            GROUP BY 1, 2 ORDER BY 1, 2""",
        "mart_city_ranking": """
            SELECT c.city_name, c.state, ROUND(AVG(f.pm2_5), 2) AS avg_pm25,
                   RANK() OVER (ORDER BY AVG(f.pm2_5) DESC) AS pollution_rank
            FROM fact_air_quality f JOIN dim_city c USING (city_key)
            GROUP BY 1, 2 ORDER BY pollution_rank""",
        "mart_hourly_pattern": """
            SELECT t.hour, ROUND(AVG(f.pm2_5), 2) AS avg_pm25
            FROM fact_air_quality f JOIN dim_time t USING (time_key)
            GROUP BY 1 ORDER BY 1""",
    }
    for name, sql in marts.items():
        con.execute(f"CREATE OR REPLACE TABLE {name} AS {sql}")
        con.execute(f"COPY {name} TO '{config.CURATED_DIR / (name + '.parquet')}' (FORMAT PARQUET)")

    counts = {t: con.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
              for t in ["dim_city", "dim_time", "fact_air_quality", *marts]}
    con.close()
    log.info("warehouse loaded: %s", counts)
    return counts


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    run()
