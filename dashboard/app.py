"""Streamlit dashboard served from the curated warehouse.

    streamlit run dashboard/app.py
"""
import sys
from pathlib import Path

import duckdb
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from pipeline import config  # noqa: E402

st.set_page_config(page_title="India Air Quality", layout="wide")
st.title("India Air Quality - Data Engineering Pipeline")

if not config.WAREHOUSE_PATH.exists():
    st.error("Warehouse not found. Run: `python -m pipeline.run_pipeline` first.")
    st.stop()

con = duckdb.connect(str(config.WAREHOUSE_PATH), read_only=True)
ranking = con.execute("SELECT * FROM mart_city_ranking").df()
daily = con.execute("SELECT * FROM mart_city_daily").df()
hourly = con.execute("SELECT * FROM mart_hourly_pattern").df()

c1, c2, c3 = st.columns(3)
c1.metric("Most polluted city", ranking.iloc[0]["city_name"], f"{ranking.iloc[0]['avg_pm25']} ug/m3 PM2.5")
c2.metric("Cleanest city", ranking.iloc[-1]["city_name"], f"{ranking.iloc[-1]['avg_pm25']} ug/m3 PM2.5")
c3.metric("Fact rows", f"{con.execute('SELECT COUNT(*) FROM fact_air_quality').fetchone()[0]:,}")

left, right = st.columns(2)
with left:
    st.subheader("Average PM2.5 by city")
    st.bar_chart(ranking.set_index("city_name")["avg_pm25"])
with right:
    st.subheader("PM2.5 by hour of day (rush-hour peaks)")
    st.line_chart(hourly.set_index("hour")["avg_pm25"])

st.subheader("Daily PM2.5 trend")
cities = st.multiselect("Cities", sorted(daily["city_name"].unique()),
                        default=list(daily["city_name"].unique())[:3])
pivot = daily[daily["city_name"].isin(cities)].pivot(index="date", columns="city_name", values="avg_pm25")
st.line_chart(pivot)

st.subheader("Data quality report")
if config.QUALITY_REPORT_PATH.exists():
    import json
    st.dataframe(json.loads(config.QUALITY_REPORT_PATH.read_text()), width="stretch")
