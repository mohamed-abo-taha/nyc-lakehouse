"""Streamlit dashboard on the gold marts (reads the DuckDB warehouse directly).

A lightweight, dependency-free alternative to Metabase for verifying the star
schema serves real analytics. Run:  streamlit run dashboard/app.py
"""

from __future__ import annotations

import sys
from pathlib import Path

import duckdb
import pandas as pd
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from pipeline.config import SETTINGS

st.set_page_config(page_title="NYC Trips Lakehouse", layout="wide")


@st.cache_resource
def con():
    return duckdb.connect(str(SETTINGS.warehouse_db), read_only=True)


def q(sql: str) -> pd.DataFrame:
    return con().execute(sql).fetchdf()


st.title("NYC Yellow Taxi — gold marts")

kpis = q("""
    select count(*) as trips,
           sum(total_amount) as revenue,
           avg(fare_amount) as avg_fare,
           avg(trip_distance) as avg_distance
    from gold.fct_trips
""").iloc[0]
c1, c2, c3, c4 = st.columns(4)
c1.metric("Trips", f"{int(kpis.trips):,}")
c2.metric("Revenue", f"${kpis.revenue/1e6:,.1f}M")
c3.metric("Avg fare", f"${kpis.avg_fare:,.2f}")
c4.metric("Avg distance", f"{kpis.avg_distance:,.2f} mi")

st.subheader("Trips per day")
st.line_chart(q("""
    select d.date_day as day, count(*) as trips
    from gold.fct_trips f join gold.dim_date d on f.date_key = d.date_key
    group by 1 order by 1
"""), x="day", y="trips")

left, right = st.columns(2)
with left:
    st.subheader("Revenue by pickup borough")
    st.bar_chart(q("""
        select z.borough, sum(f.total_amount) as revenue
        from gold.fct_trips f join gold.dim_zone z on f.pickup_zone_id = z.zone_id
        group by 1 order by 2 desc
    """), x="borough", y="revenue")
with right:
    st.subheader("Payment mix")
    st.bar_chart(q("""
        select p.payment_type_name as payment, count(*) as trips
        from gold.fct_trips f
        join gold.dim_payment_type p on f.payment_type_id = p.payment_type_id
        group by 1 order by 2 desc
    """), x="payment", y="trips")

st.caption("Source: gold.fct_trips + conformed dimensions (dbt on DuckDB over the MinIO lake).")
