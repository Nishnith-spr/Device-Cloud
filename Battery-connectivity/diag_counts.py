import sys
import os
import pandas as pd

PROJECT_ROOT = os.path.abspath(os.path.join(os.getcwd(), ".."))
sys.path.insert(0, PROJECT_ROOT)

from aws_db_conn import get_athena_client
from aws_db_exec import run_query, fetch_df
from aws_db_creds import DATABASE, S3_STAGING_DIR

client = get_athena_client()

print("\n--- Diagnostic: Status and Time distribution ---")
# Get statuses for Jan and Feb
q = """
SELECT 
    DATE_TRUNC('month', CAST(created_time AS DATE)) as onboarded_month,
    COUNT(DISTINCT upper(trim(battery_oem_no))) as unique_batteries_added
FROM "AWSDataCatalog"."prod_staging_db"."atlas_battery_360_events"
WHERE battery_oem_no IS NOT NULL
  AND length(trim(battery_oem_no)) IN (15, 18)
  AND substr(upper(trim(battery_oem_no)), 1, 1) IN ('U', 'A', 'C', '7', '8', 'D')
  and country_code is not null
GROUP BY 1
ORDER BY 1
"""
qid = run_query(client, q, DATABASE, S3_STAGING_DIR)
df = fetch_df(client, qid)

pd.set_option('display.max_rows', 60)
print(df.to_string(index=False))

# Cumulative by month
df['unique_batteries_added'] = pd.to_numeric(df['unique_batteries_added'], errors='coerce')
df['cumulative'] = df['unique_batteries_added'].cumsum()
print("\n--- Cumulative (New batteries per month + running total) ---")
print(df[['onboarded_month', 'unique_batteries_added', 'cumulative']].to_string(index=False))

# --------------------------------------------------------------------------
# PART 2: Weekly snapshot – cumulative total as of each week end
# --------------------------------------------------------------------------
print("\n--- Weekly: All-time Cumulative Battery Count & Increment ---")
q2 = """
WITH weekly_new AS (
    SELECT 
        DATE_TRUNC('week', CAST(created_time AS DATE)) AS week_start,
        COUNT(DISTINCT upper(trim(battery_oem_no))) AS new_this_week
    FROM "AWSDataCatalog"."prod_staging_db"."atlas_battery_360_events"
    WHERE battery_oem_no IS NOT NULL
      AND country_code IS NOT NULL
      AND length(trim(battery_oem_no)) IN (15, 18)
      AND substr(upper(trim(battery_oem_no)), 1, 1) IN ('U', 'A', 'C', '7', '8', 'D')
      AND created_time IS NOT NULL
    GROUP BY 1
)
SELECT 
    week_start,
    new_this_week,
    SUM(new_this_week) OVER (ORDER BY week_start ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS total_cumulative
FROM weekly_new
WHERE week_start >= DATE '2025-11-01'
ORDER BY week_start
"""
qid2 = run_query(client, q2, DATABASE, S3_STAGING_DIR)
df2 = fetch_df(client, qid2)
df2['new_this_week'] = pd.to_numeric(df2['new_this_week'], errors='coerce')
df2['total_cumulative'] = pd.to_numeric(df2['total_cumulative'], errors='coerce')
df2['wk_over_wk_change'] = df2['new_this_week'].diff().fillna(df2['new_this_week'])
df2['wk_over_wk_change'] = df2['wk_over_wk_change'].apply(lambda x: f"+{int(x)}" if x >= 0 else str(int(x)))
print(df2.to_string(index=False))


