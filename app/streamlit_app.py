"""Read-only results viewer for the NYC Taxi Iceberg pipeline.

Shows PRE-EXPORTED Snowflake query results (CSV files in results/), so the app
never needs, and never exposes, any credentials.

Local:   streamlit run app/streamlit_app.py
Hosted:  Streamlit Community Cloud -> New app -> this repo -> app/streamlit_app.py
"""
import json
from pathlib import Path

import pandas as pd
import streamlit as st

RESULTS = Path(__file__).resolve().parent.parent / "results"

st.set_page_config(page_title="NYC Taxi - Iceberg Pipeline Results", layout="wide")
st.title("NYC Yellow Taxi: demand and revenue by zone")
st.caption(
    "S3 (raw) -> PySpark -> Apache Iceberg (Polaris catalog) -> Snowflake. "
    "Results below were exported from Snowflake after the pipeline ran."
)


def load(name: str) -> pd.DataFrame | None:
    path = RESULTS / name
    if not path.exists():
        st.warning(f"Missing {path.name}. Run sql/queries.sql in Snowflake and export the CSV to results/.")
        return None
    df = pd.read_csv(path)
    df.columns = [c.lower() for c in df.columns]  # Snowflake exports UPPERCASE headers
    return df


tab1, tab2, tab3, tab4 = st.tabs(
    ["Top 10 zones by revenue", "Demand by hour", "Tip % by payment type", "Data quality"]
)

with tab1:
    df = load("q1_top10_revenue_zones.csv")
    if df is not None:
        st.bar_chart(df, x="pickup_zone", y="total_revenue", horizontal=True)
        st.dataframe(df, hide_index=True, width="stretch")

with tab2:
    df = load("q2_demand_by_hour.csv")
    if df is not None:
        st.bar_chart(df, x="pickup_hour", y="trips")
        st.dataframe(df, hide_index=True, width="stretch")

with tab3:
    df = load("q3_tip_pct_by_payment_type.csv")
    if df is not None:
        st.bar_chart(df, x="payment_type_name", y="avg_tip_pct")
        st.dataframe(df, hide_index=True, width="stretch")
        st.info("Cash tips are generally not recorded in TLC data, so cash shows ~0%.")

with tab4:
    path = RESULTS / "dq_report.json"
    if path.exists():
        dq = pd.DataFrame(json.loads(path.read_text()))
        dq["result"] = dq["passed"].map({True: "PASS", False: "FAIL"})
        st.dataframe(dq[["name", "severity", "result", "detail"]], hide_index=True, width="stretch")
    else:
        st.warning("Missing dq_report.json. Run `make dq` and commit results/dq_report.json.")
    report = RESULTS / "iceberg_features_report.txt"
    if report.exists():
        st.subheader("Iceberg schema evolution + time travel")
        st.code(report.read_text())
