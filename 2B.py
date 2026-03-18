from google.cloud import bigquery
import pandas as pd

client = bigquery.Client(project="oss-sustainability-study")
REPOS_TABLE         = "oss-sustainability-study.fork_study.repos"
EARLY_PERIOD_MONTHS = 6

# ---------------------------------------------------------------
# Your forks span 2023-12 to 2024-01
# Early period = +6 months, so scan up to 2024-07
# ---------------------------------------------------------------
START_YEAR_MONTH = "202312"
END_YEAR_MONTH   = "202407"

# ---------------------------------------------------------------
# DRY RUN first — check cost before actually running
# ---------------------------------------------------------------
def estimate_cost(sql, label):
    job_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
    job = client.query(sql, job_config=job_config)
    gb = job.total_bytes_processed / (1024 ** 3)
    print(f"{label}: {gb:.2f} GB scanned")
    return gb

# ---------------------------------------------------------------
# OPTIMISED QUERY STRUCTURE:
# Step 1 — filter GH Archive by table suffix AND event type first
# Step 2 — then join to repos (much cheaper)
# ---------------------------------------------------------------
activity_sql = f"""
WITH 
-- Step 1: Pull only relevant events from GH Archive for the date window
-- This is the expensive scan — but we minimise it by filtering early
gh_events AS (
  SELECT
    repo.name   AS repo_name,
    actor.login AS actor_login,
    type,
    created_at,
    payload
  FROM `githubarchive.month.*`
  WHERE _TABLE_SUFFIX BETWEEN '{START_YEAR_MONTH}' AND '{END_YEAR_MONTH}'
    AND type IN ('PushEvent','IssuesEvent','PullRequestEvent','ReleaseEvent','WatchEvent','ForkEvent')
),

-- Step 2: Join to your repos and apply early period filter
repo_events AS (
  SELECT
    e.repo_name,
    e.actor_login,
    e.type,
    e.created_at,
    e.payload
  FROM gh_events e
  INNER JOIN `{REPOS_TABLE}` r ON r.repo_name = e.repo_name
  WHERE e.created_at BETWEEN r.created_at
      AND TIMESTAMP_ADD(r.created_at, INTERVAL 182 DAY)
),

commits AS (
  SELECT
    repo_name,
    COUNT(*)                    AS total_commits,
    COUNT(DISTINCT actor_login) AS unique_commit_authors
  FROM repo_events
  WHERE type = 'PushEvent'
  GROUP BY repo_name
),

issues AS (
  SELECT
    repo_name,
    COUNTIF(JSON_EXTRACT_SCALAR(payload, '$.action') = 'opened') AS issues_opened,
    COUNTIF(JSON_EXTRACT_SCALAR(payload, '$.action') = 'closed') AS issues_closed
  FROM repo_events
  WHERE type = 'IssuesEvent'
  GROUP BY repo_name
),

prs AS (
  SELECT
    repo_name,
    COUNTIF(JSON_EXTRACT_SCALAR(payload, '$.action') = 'opened') AS prs_opened,
    COUNTIF(JSON_EXTRACT_SCALAR(payload, '$.action') = 'closed'
      AND JSON_EXTRACT_SCALAR(payload, '$.pull_request.merged') = 'true')  AS prs_merged,
    COUNTIF(JSON_EXTRACT_SCALAR(payload, '$.action') = 'closed'
      AND JSON_EXTRACT_SCALAR(payload, '$.pull_request.merged') = 'false') AS prs_rejected
  FROM repo_events
  WHERE type = 'PullRequestEvent'
  GROUP BY repo_name
),

releases AS (
  SELECT repo_name, COUNT(*) AS num_releases
  FROM repo_events
  WHERE type = 'ReleaseEvent'
    AND JSON_EXTRACT_SCALAR(payload, '$.action') = 'published'
  GROUP BY repo_name
),

stars AS (
  SELECT repo_name, COUNT(*) AS star_count
  FROM repo_events
  WHERE type = 'WatchEvent'
    AND JSON_EXTRACT_SCALAR(payload, '$.action') = 'started'
  GROUP BY repo_name
),

forks AS (
  SELECT repo_name, COUNT(*) AS fork_count
  FROM repo_events
  WHERE type = 'ForkEvent'
  GROUP BY repo_name
)

SELECT
  r.repo_name,
  COALESCE(c.total_commits, 0)         AS total_commits,
  COALESCE(c.unique_commit_authors, 0) AS unique_commit_authors,
  COALESCE(i.issues_opened, 0)         AS issues_opened,
  COALESCE(i.issues_closed, 0)         AS issues_closed,
  COALESCE(p.prs_opened, 0)            AS prs_opened,
  COALESCE(p.prs_merged, 0)            AS prs_merged,
  COALESCE(p.prs_rejected, 0)          AS prs_rejected,
  COALESCE(rel.num_releases, 0)        AS num_releases,
  COALESCE(s.star_count, 0)            AS star_count,
  COALESCE(f.fork_count, 0)            AS fork_count
FROM `{REPOS_TABLE}` r
LEFT JOIN commits  c   ON r.repo_name = c.repo_name
LEFT JOIN issues   i   ON r.repo_name = i.repo_name
LEFT JOIN prs      p   ON r.repo_name = p.repo_name
LEFT JOIN releases rel ON r.repo_name = rel.repo_name
LEFT JOIN stars    s   ON r.repo_name = s.repo_name
LEFT JOIN forks    f   ON r.repo_name = f.repo_name
"""

# Check cost first
gb = estimate_cost(activity_sql, "Activity Metrics")

if gb > 500:
    print(f"\n⚠️  Still too expensive ({gb:.0f} GB). Do not run yet — contact for further optimisation.")
else:
    print(f"\n✅ Cost acceptable. Running query now...")
    df_activity = client.query(activity_sql).to_dataframe()
    df_activity.to_csv("step2b_activity.csv", index=False)
    print(f"Done! {len(df_activity)} repos.")
    print(f"Repos with at least 1 commit: {(df_activity['total_commits'] > 0).sum()}")
    print(f"Repos with zero activity:     {(df_activity['total_commits'] == 0).sum()}")
    print(df_activity[df_activity["total_commits"] > 0].head(10))