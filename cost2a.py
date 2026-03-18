"""
Run this BEFORE any of the real queries.
It estimates how much data each query will scan without actually running it.
BigQuery free tier = 1TB/month. This tells you how much you'll use.
"""
from google.cloud import bigquery
import pandas as pd

client = bigquery.Client(project="oss-sustainability-study")
REPOS_TABLE = "oss-sustainability-study.fork_study.repos"

# ---------------------------------------------------------------
# First, find the date range of your repos so we can limit
# GH Archive scans to only relevant months
# ---------------------------------------------------------------
date_range_sql = f"""
SELECT
  MIN(created_at) AS earliest_fork,
  MAX(created_at) AS latest_fork,
  DATE_DIFF(MAX(DATE(created_at)), MIN(DATE(created_at)), MONTH) AS span_months
FROM `{REPOS_TABLE}`
"""
df_dates = client.query(date_range_sql).to_dataframe()
print("=== Your Repo Date Range ===")
print(df_dates)

earliest = df_dates["earliest_fork"][0]
latest   = df_dates["latest_fork"][0]
print(f"\nYour forks span from {earliest} to {latest}")
print("Use these dates to set START_YEAR_MONTH and END_YEAR_MONTH below")
print("Format: 'YYYYMM' e.g. '202301' for January 2023\n")

# ---------------------------------------------------------------
# SET THESE based on the output above
# Add 6 months to latest_fork to cover the early period window
# e.g. if latest fork is 2023-12, set END to 202406
# ---------------------------------------------------------------
START_YEAR_MONTH = "202301"   # 👈 change this
END_YEAR_MONTH   = "202406"   # 👈 change this (latest fork + 6 months)

# ---------------------------------------------------------------
# DRY RUN: estimates bytes scanned without running the query
# ---------------------------------------------------------------
def estimate_cost(query_name, sql):
    job_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
    job = client.query(sql, job_config=job_config)
    bytes_scanned = job.total_bytes_processed
    gb_scanned    = bytes_scanned / (1024 ** 3)
    tb_scanned    = bytes_scanned / (1024 ** 4)
    print(f"\n{'='*50}")
    print(f"Query: {query_name}")
    print(f"  Data scanned: {gb_scanned:.2f} GB ({tb_scanned:.4f} TB)")
    print(f"  Free tier:    1 TB/month")
    print(f"  % of free tier used: {tb_scanned * 100:.2f}%")
    if tb_scanned > 0.8:
        print("  ⚠️  WARNING: This query uses >80% of your free tier!")
    else:
        print("  ✅ Safe to run")
    return tb_scanned

# Activity metrics query (dry run)
activity_sql = f"""
SELECT e.repo.name, e.type, e.created_at, e.actor.login, e.payload
FROM `githubarchive.month.*` e
JOIN `{REPOS_TABLE}` r ON r.repo_name = e.repo.name
WHERE e.type IN ('PushEvent','IssuesEvent','PullRequestEvent','ReleaseEvent','WatchEvent','ForkEvent')
  AND _TABLE_SUFFIX BETWEEN '{START_YEAR_MONTH}' AND '{END_YEAR_MONTH}'
"""

# Responsiveness query (dry run)
responsiveness_sql = f"""
SELECT e.repo.name, e.type, e.created_at, e.payload
FROM `githubarchive.month.*` e
JOIN `{REPOS_TABLE}` r ON r.repo_name = e.repo.name
WHERE e.type IN ('IssueCommentEvent','PullRequestReviewCommentEvent')
  AND _TABLE_SUFFIX BETWEEN '{START_YEAR_MONTH}' AND '{END_YEAR_MONTH}'
"""

# Contributor time series query (dry run)
contributor_sql = f"""
SELECT e.repo.name, e.actor.login, e.created_at
FROM `githubarchive.month.*` e
JOIN `{REPOS_TABLE}` r ON r.repo_name = e.repo.name
WHERE e.type = 'PushEvent'
  AND _TABLE_SUFFIX BETWEEN '{START_YEAR_MONTH}' AND '{END_YEAR_MONTH}'
"""

print("\n=== Cost Estimates ===")
t1 = estimate_cost("Activity Metrics",          activity_sql)
t2 = estimate_cost("Responsiveness Metrics",    responsiveness_sql)
t3 = estimate_cost("Contributor Time Series",   contributor_sql)

print(f"\n=== TOTAL ESTIMATED USAGE ===")
print(f"  Total: {(t1+t2+t3)*1000:.2f} GB  ({(t1+t2+t3)*100:.2f}% of free tier)")
print(f"\nIf total is under 100%, you're safe to run all three queries this month.")
