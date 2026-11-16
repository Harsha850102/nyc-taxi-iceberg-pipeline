# Results

Put the files produced by the pipeline here and commit them:

| File | Produced by |
|---|---|
| `q1_top10_revenue_zones.csv` | Snowsight: run Q1 in `sql/queries.sql` → Download → CSV |
| `q2_demand_by_hour.csv` | Snowsight: Q2 |
| `q3_tip_pct_by_payment_type.csv` | Snowsight: Q3 |
| `snowflake_q1.png`, `snowflake_q2.png`, `snowflake_q3.png` | Screenshots of each query result in Snowsight |
| `dq_report.json` | `make dq` (written automatically) |
| `iceberg_features_report.txt` | `make features` (written automatically) |

The Streamlit app (`app/streamlit_app.py`) reads these files; it never connects to Snowflake.
