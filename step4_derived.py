import pandas as pd
import numpy as np
import os

# ---------------------------------------------------------------
# LOAD ALL DATA
# ---------------------------------------------------------------
print("Loading data files...")

df_activity   = pd.read_csv("step2b_activity.csv")
df_resp       = pd.read_csv("step2c_responsiveness.csv")
df_contrib_ts = pd.read_csv("step2d_contributors.csv")
df_governance = pd.read_csv("step3_governance_metadata.csv")
df_repos      = pd.read_csv("repos_table.csv")

print(f"  Activity:       {len(df_activity)} repos")
print(f"  Responsiveness: {len(df_resp)} repos")
print(f"  Contributors:   {len(df_contrib_ts)} records")
print(f"  Governance:     {len(df_governance)} repos")
print(f"  Repos table:    {len(df_repos)} repos")

# Sort contributor time series chronologically
df_contrib_ts = df_contrib_ts.sort_values(["repo_name", "month"])

# ---------------------------------------------------------------
# 1. BUS FACTOR
# Minimum number of contributors responsible for >= 80% of commits
# ---------------------------------------------------------------
print("\nComputing bus factor...")

def bus_factor(group):
    total = group["commit_count"].sum()
    if total == 0:
        return 0
    sorted_contribs = group["commit_count"].sort_values(ascending=False)
    cumulative = sorted_contribs.cumsum()
    n = (cumulative < total * 0.8).sum() + 1
    return n

bus_factor_df = (
    df_contrib_ts
    .groupby("repo_name")
    .apply(bus_factor)
    .reset_index()
    .rename(columns={0: "bus_factor"})
)

# ---------------------------------------------------------------
# 2. CONTRIBUTOR RETENTION
# % of month-1 contributors still contributing by month 3 and 6
# ---------------------------------------------------------------
print("Computing contributor retention...")

def contributor_retention(group):
    months = sorted(group["month"].unique())
    if len(months) < 1:
        return pd.Series({"retention_m3": None, "retention_m6": None})

    m1 = months[0]
    contribs_m1 = set(group[group["month"] == m1]["contributor"])

    if len(contribs_m1) == 0:
        return pd.Series({"retention_m3": None, "retention_m6": None})

    retention_m3 = None
    retention_m6 = None

    if len(months) >= 3:
        m3 = months[2]
        contribs_m3 = set(group[group["month"] == m3]["contributor"])
        retention_m3 = len(contribs_m1 & contribs_m3) / len(contribs_m1)

    if len(months) >= 6:
        m6 = months[5]
        contribs_m6 = set(group[group["month"] == m6]["contributor"])
        retention_m6 = len(contribs_m1 & contribs_m6) / len(contribs_m1)

    return pd.Series({"retention_m3": retention_m3, "retention_m6": retention_m6})

retention_df = (
    df_contrib_ts
    .groupby("repo_name")
    .apply(contributor_retention)
    .reset_index()
)

# ---------------------------------------------------------------
# 3. TIME TO FIRST EXTERNAL CONTRIBUTION
# First month where someone other than the first contributor appears
# ---------------------------------------------------------------
print("Computing time to first external contribution...")

def time_to_first_external(group):
    months = sorted(group["month"].unique())
    if len(months) == 0:
        return None
    m1_contribs = group[group["month"] == months[0]]["contributor"].tolist()
    founder = m1_contribs[0] if m1_contribs else None
    for i, month in enumerate(months):
        contribs = set(group[group["month"] == month]["contributor"])
        if founder and (contribs - {founder}):
            return i
    return None

ext_contrib_df = (
    df_contrib_ts
    .groupby("repo_name")
    .apply(time_to_first_external)
    .reset_index()
    .rename(columns={0: "months_to_first_external_contrib"})
)

# ---------------------------------------------------------------
# 4. GINI COEFFICIENT
# Measures inequality of contribution distribution
# 0 = perfectly equal, 1 = one person does everything
# ---------------------------------------------------------------
print("Computing Gini coefficient...")

def gini(group):
    vals = group["commit_count"].values.astype(float)
    if vals.sum() == 0 or len(vals) < 2:
        return None
    vals = np.sort(vals)
    n = len(vals)
    idx = np.arange(1, n + 1)
    return (2 * np.sum(idx * vals) / (n * vals.sum())) - (n + 1) / n

gini_df = (
    df_contrib_ts
    .groupby("repo_name")
    .apply(gini)
    .reset_index()
    .rename(columns={0: "contributor_gini"})
)

# ---------------------------------------------------------------
# 5. PR ACCEPTANCE RATE & ISSUE CLOSE RATE
# ---------------------------------------------------------------
print("Computing PR acceptance rate and issue close rate...")

df_activity["pr_acceptance_rate"] = np.where(
    df_activity["prs_opened"] > 0,
    df_activity["prs_merged"] / df_activity["prs_opened"],
    np.nan
)

df_activity["issue_close_rate"] = np.where(
    df_activity["issues_opened"] > 0,
    df_activity["issues_closed"] / df_activity["issues_opened"],
    np.nan
)

# ---------------------------------------------------------------
# 6. COMMIT FREQUENCY PER WEEK
# 182 days early period = 26 weeks
# ---------------------------------------------------------------
EARLY_PERIOD_WEEKS = 26
df_activity["commit_frequency_per_week"] = (
    df_activity["total_commits"] / EARLY_PERIOD_WEEKS
)

# ---------------------------------------------------------------
# 7. MERGE EVERYTHING
# ---------------------------------------------------------------
print("\nMerging all data...")

df_final = (
    df_repos[["repo_name", "is_fork", "fork_owner_type", "created_at"]]
    .merge(df_activity,    on="repo_name", how="left")
    .merge(df_resp,        on="repo_name", how="left")
    .merge(df_governance,  on="repo_name", how="left")
    .merge(bus_factor_df,  on="repo_name", how="left")
    .merge(retention_df,   on="repo_name", how="left")
    .merge(ext_contrib_df, on="repo_name", how="left")
    .merge(gini_df,        on="repo_name", how="left")
)

# ---------------------------------------------------------------
# 8. SELECT AND ORDER FINAL COLUMNS
# ---------------------------------------------------------------
feature_cols = [
    # Identifiers
    "repo_name", "is_fork", "fork_owner_type", "created_at",

    # Activity
    "total_commits", "commit_frequency_per_week",
    "unique_commit_authors",
    "issues_opened", "issues_closed", "issue_close_rate",
    "prs_opened", "prs_merged", "prs_rejected", "pr_acceptance_rate",
    "num_releases", "star_count", "fork_count",
    "total_issue_comments", "total_pr_comments",

    # Responsiveness
    "avg_issue_first_response_hrs", "avg_pr_first_response_hrs",

    # Contributor health
    "bus_factor", "contributor_gini",
    "retention_m3", "retention_m6",
    "months_to_first_external_contrib",

    # Governance
    "has_contributing", "has_code_of_conduct", "has_license", "has_cicd",

    # Fork-specific
    "parent_repo", "parent_stars", "parent_forks",
    "parent_created_at", "parent_language",
    "early_commits_on_branch", "parent_early_commits", "divergence_ratio",

    # Language
    "primary_language", "topics",
]

# Keep only columns that exist
feature_cols = [c for c in feature_cols if c in df_final.columns]
df_output = df_final[feature_cols].copy()

# ---------------------------------------------------------------
# 9. SUMMARY
# ---------------------------------------------------------------
print("\n=== Final Dataset Summary ===")
print(f"Total repos:          {len(df_output)}")
print(f"Forks:                {df_output['is_fork'].sum()}")
print(f"Non-forks:            {(~df_output['is_fork']).sum()}")
print(f"Total columns:        {len(df_output.columns)}")
print(f"\nRepos with commits:   {(df_output['total_commits'] > 0).sum()}")
print(f"Repos with PRs:       {(df_output['prs_opened'] > 0).sum()}")
print(f"Repos with issues:    {(df_output['issues_opened'] > 0).sum()}")
print(f"Repos with CI/CD:     {df_output['has_cicd'].sum()}")
print(f"Repos with license:   {df_output['has_license'].sum()}")

print(f"\nMissing values per column (top 10):")
print(df_output.isnull().sum().sort_values(ascending=False).head(10))

# Save
df_output.to_csv("final_dataset.csv", index=False)
print(f"\n✅ Saved to final_dataset.csv")
print(f"   Shape: {df_output.shape[0]} rows × {df_output.shape[1]} columns")