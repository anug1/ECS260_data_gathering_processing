from google.cloud import bigquery
import pandas as pd

client = bigquery.Client(project="oss-sustainability-study")

# ---------------------------------------------------------------
# CONFIG
# Define your early period window (months after fork/creation)
# ---------------------------------------------------------------
EARLY_PERIOD_MONTHS = 6
REPOS_TABLE = "oss-sustainability-study.fork_study.repos"  # your list of repos with columns: repo_name, created_at, is_fork


# ---------------------------------------------------------------
# HELPER: run a query and return a dataframe
# ---------------------------------------------------------------
def run_query(sql):
    return client.query(sql).to_dataframe()


# ---------------------------------------------------------------
# 1. ACTIVITY METRICS
# Commits, PRs, Issues, Releases in the early period
# ---------------------------------------------------------------
activity_sql = f"""
WITH repos AS (
  SELECT repo_name, created_at, is_fork
  FROM `{REPOS_TABLE}`
),

-- Commits (PushEvent)
commits AS (
  SELECT
    e.repo.name AS repo_name,
    COUNT(*) AS total_commits,
    COUNT(DISTINCT JSON_EXTRACT_SCALAR(p, '$.author.email')) AS commit_authors
  FROM `githubarchive.month.*` e,
  UNNEST(JSON_EXTRACT_ARRAY(e.payload, '$.commits')) AS p
  JOIN repos r ON r.repo_name = e.repo.name
  WHERE e.type = 'PushEvent'
    AND e.created_at BETWEEN r.created_at
        AND TIMESTAMP_ADD(r.created_at, INTERVAL {EARLY_PERIOD_MONTHS} MONTH)
  GROUP BY repo_name
),

-- Issues
issues AS (
  SELECT
    e.repo.name AS repo_name,
    COUNTIF(JSON_EXTRACT_SCALAR(e.payload, '$.action') = 'opened') AS issues_opened,
    COUNTIF(JSON_EXTRACT_SCALAR(e.payload, '$.action') = 'closed') AS issues_closed
  FROM `githubarchive.month.*` e
  JOIN repos r ON r.repo_name = e.repo.name
  WHERE e.type = 'IssuesEvent'
    AND e.created_at BETWEEN r.created_at
        AND TIMESTAMP_ADD(r.created_at, INTERVAL {EARLY_PERIOD_MONTHS} MONTH)
  GROUP BY repo_name
),

-- Pull Requests
prs AS (
  SELECT
    e.repo.name AS repo_name,
    COUNTIF(JSON_EXTRACT_SCALAR(e.payload, '$.action') = 'opened') AS prs_opened,
    COUNTIF(JSON_EXTRACT_SCALAR(e.payload, '$.action') = 'closed'
      AND JSON_EXTRACT_SCALAR(e.payload, '$.pull_request.merged') = 'true') AS prs_merged,
    COUNTIF(JSON_EXTRACT_SCALAR(e.payload, '$.action') = 'closed'
      AND JSON_EXTRACT_SCALAR(e.payload, '$.pull_request.merged') = 'false') AS prs_rejected
  FROM `githubarchive.month.*` e
  JOIN repos r ON r.repo_name = e.repo.name
  WHERE e.type = 'PullRequestEvent'
    AND e.created_at BETWEEN r.created_at
        AND TIMESTAMP_ADD(r.created_at, INTERVAL {EARLY_PERIOD_MONTHS} MONTH)
  GROUP BY repo_name
),

-- Releases / Tags
releases AS (
  SELECT
    e.repo.name AS repo_name,
    COUNT(*) AS num_releases
  FROM `githubarchive.month.*` e
  JOIN repos r ON r.repo_name = e.repo.name
  WHERE e.type = 'ReleaseEvent'
    AND JSON_EXTRACT_SCALAR(e.payload, '$.action') = 'published'
    AND e.created_at BETWEEN r.created_at
        AND TIMESTAMP_ADD(r.created_at, INTERVAL {EARLY_PERIOD_MONTHS} MONTH)
  GROUP BY repo_name
),

-- Stars
stars AS (
  SELECT
    e.repo.name AS repo_name,
    COUNT(*) AS star_count
  FROM `githubarchive.month.*` e
  JOIN repos r ON r.repo_name = e.repo.name
  WHERE e.type = 'WatchEvent'
    AND JSON_EXTRACT_SCALAR(e.payload, '$.action') = 'started'
    AND e.created_at BETWEEN r.created_at
        AND TIMESTAMP_ADD(r.created_at, INTERVAL {EARLY_PERIOD_MONTHS} MONTH)
  GROUP BY repo_name
),

-- Forks of forks (downstream adoption)
forks AS (
  SELECT
    e.repo.name AS repo_name,
    COUNT(*) AS fork_count
  FROM `githubarchive.month.*` e
  JOIN repos r ON r.repo_name = e.repo.name
  WHERE e.type = 'ForkEvent'
    AND e.created_at BETWEEN r.created_at
        AND TIMESTAMP_ADD(r.created_at, INTERVAL {EARLY_PERIOD_MONTHS} MONTH)
  GROUP BY repo_name
)

SELECT
  r.repo_name,
  r.is_fork,
  COALESCE(c.total_commits, 0)    AS total_commits,
  COALESCE(c.commit_authors, 0)   AS unique_commit_authors,
  COALESCE(i.issues_opened, 0)    AS issues_opened,
  COALESCE(i.issues_closed, 0)    AS issues_closed,
  COALESCE(p.prs_opened, 0)       AS prs_opened,
  COALESCE(p.prs_merged, 0)       AS prs_merged,
  COALESCE(p.prs_rejected, 0)     AS prs_rejected,
  COALESCE(rel.num_releases, 0)   AS num_releases,
  COALESCE(s.star_count, 0)       AS star_count,
  COALESCE(f.fork_count, 0)       AS fork_count
FROM repos r
LEFT JOIN commits   c   ON r.repo_name = c.repo_name
LEFT JOIN issues    i   ON r.repo_name = i.repo_name
LEFT JOIN prs       p   ON r.repo_name = p.repo_name
LEFT JOIN releases  rel ON r.repo_name = rel.repo_name
LEFT JOIN stars     s   ON r.repo_name = s.repo_name
LEFT JOIN forks     f   ON r.repo_name = f.repo_name
"""

# ---------------------------------------------------------------
# 2. RESPONSIVENESS METRICS
# First response time on issues and PRs
# ---------------------------------------------------------------
responsiveness_sql = f"""
WITH repos AS (
  SELECT repo_name, created_at FROM `{REPOS_TABLE}`
),

-- First comment on each issue (proxy for first response time)
issue_first_response AS (
  SELECT
    e.repo.name AS repo_name,
    JSON_EXTRACT_SCALAR(e.payload, '$.issue.number') AS issue_number,
    MIN(e.created_at) AS first_comment_at,
    MIN(CAST(JSON_EXTRACT_SCALAR(e.payload, '$.issue.created_at') AS TIMESTAMP)) AS issue_opened_at
  FROM `githubarchive.month.*` e
  JOIN repos r ON r.repo_name = e.repo.name
  WHERE e.type = 'IssueCommentEvent'
    AND e.created_at BETWEEN r.created_at
        AND TIMESTAMP_ADD(r.created_at, INTERVAL {EARLY_PERIOD_MONTHS} MONTH)
  GROUP BY repo_name, issue_number
),

-- PR first review comment
pr_first_response AS (
  SELECT
    e.repo.name AS repo_name,
    JSON_EXTRACT_SCALAR(e.payload, '$.pull_request.number') AS pr_number,
    MIN(e.created_at) AS first_review_at,
    MIN(CAST(JSON_EXTRACT_SCALAR(e.payload, '$.pull_request.created_at') AS TIMESTAMP)) AS pr_opened_at
  FROM `githubarchive.month.*` e
  JOIN repos r ON r.repo_name = e.repo.name
  WHERE e.type = 'PullRequestReviewCommentEvent'
    AND e.created_at BETWEEN r.created_at
        AND TIMESTAMP_ADD(r.created_at, INTERVAL {EARLY_PERIOD_MONTHS} MONTH)
  GROUP BY repo_name, pr_number
)

SELECT
  r.repo_name,
  -- Issue responsiveness (in hours)
  AVG(TIMESTAMP_DIFF(ifr.first_comment_at, ifr.issue_opened_at, HOUR))  AS avg_issue_first_response_hrs,
  -- PR responsiveness (in hours)
  AVG(TIMESTAMP_DIFF(pfr.first_review_at, pfr.pr_opened_at, HOUR))      AS avg_pr_first_response_hrs
FROM repos r
LEFT JOIN issue_first_response ifr ON r.repo_name = ifr.repo_name
LEFT JOIN pr_first_response    pfr ON r.repo_name = pfr.repo_name
GROUP BY r.repo_name
"""

# ---------------------------------------------------------------
# 3. CONTRIBUTOR TIME SERIES (for retention & bus factor in Step 4)
# Get per-contributor commit counts per month
# ---------------------------------------------------------------
contributor_ts_sql = f"""
WITH repos AS (
  SELECT repo_name, created_at FROM `{REPOS_TABLE}`
)

SELECT
  e.repo.name AS repo_name,
  FORMAT_TIMESTAMP('%Y-%m', e.created_at) AS month,
  actor.login AS contributor,
  COUNT(*) AS commit_count
FROM `githubarchive.month.*` e
JOIN repos r ON r.repo_name = e.repo.name
WHERE e.type = 'PushEvent'
  AND e.created_at BETWEEN r.created_at
      AND TIMESTAMP_ADD(r.created_at, INTERVAL {EARLY_PERIOD_MONTHS} MONTH)
GROUP BY repo_name, month, contributor
"""

# ---------------------------------------------------------------
# RUN ALL QUERIES AND SAVE
# ---------------------------------------------------------------
print("Running activity metrics query...")
df_activity = run_query(activity_sql)

print("Running responsiveness metrics query...")
df_responsiveness = run_query(responsiveness_sql)

print("Running contributor time series query...")
df_contributors = run_query(contributor_ts_sql)

# Merge activity and responsiveness
df_step2 = df_activity.merge(df_responsiveness, on="repo_name", how="left")

# Save outputs
df_step2.to_csv("step2_metrics.csv", index=False)
df_contributors.to_csv("step2_contributor_timeseries.csv", index=False)

print(f"Step 2 done. {len(df_step2)} repos processed.")
print(df_step2.head())